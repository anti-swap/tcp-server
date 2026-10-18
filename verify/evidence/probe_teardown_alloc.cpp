// verify/evidence/probe_teardown_alloc.cpp
//
// teardownClosed() runs on the close path of every connection, and every
// function on that path is noexcept: handleRead/handleWrite -> closeConn ->
// teardownClosed. It used to queue the deferred close notification with four
// push_backs into four parallel vectors, and std::string's copy allocates.
//
// So an allocation failure there did two things at once. The exception crossed
// a noexcept boundary, which is std::terminate() -- the process dies, on the
// path that gets walked when memory is tight. And if it did not die, the batch
// was already half-inserted: flushCloseCallbacks() would hand one connection's
// id to a callback together with another connection's peer string, because the
// four vectors are only "in agreement" when all four insertions succeed.
//
// This injects that failure at exactly that place and nowhere else:
//
//   - __wrap_close  arms the injection. unregisterConn() closes the descriptor
//     a few lines before the notification is queued, and nothing between the
//     two allocates (onConnectionClosed() is a map erase under a lock).
//   - __wrap__Znwm  throws bad_alloc once, then disarms.
//
// The scenario runs in a forked child, because what is being measured is
// whether the process survives at all: the parent reports the child's status.
// After the injected close the child closes a second connection with the
// injection disarmed, which is what distinguishes "the failure was absorbed"
// from "close callbacks stopped working".
//
// Build and run (Linux only, from the repo root):
//   g++ -std=c++17 -Wall -Wextra -pthread -O1
//       -Wl,--wrap=close -Wl,--wrap=_Znwm
//       verify/evidence/probe_teardown_alloc.cpp -o probe_teardown_alloc
//   ./probe_teardown_alloc
//
// It compiles against the tree before the fix as well as the one after it: the
// counter that records an absorbed failure did not exist before, so it is
// probed for rather than named. That is the point -- the same binary has to be
// able to show the red side and the green side.
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <new>
#include <string>
#include <thread>
#include <vector>

#include "tcp-server-mt.h"

// Plain globals: they are read from inside the linker wrappers, which must have
// C linkage and therefore cannot capture anything.
int g_arm        = 0;   // the next operator new throws
int g_watchClose = 0;   // a close() arms g_arm
int g_newFailures = 0;
int g_closeCalls  = 0;  // close() calls seen while watching
int g_closeAll     = 0;  // every close() the wrapper saw, watched or not
int g_arms        = 0;  // times the injection was armed

extern "C" {
void* __real__Znwm(size_t);
void* __wrap__Znwm(size_t n) {
    if (g_arm) { g_arm = 0; ++g_newFailures; throw std::bad_alloc(); }
    return __real__Znwm(n);
}
int __real_close(int);
int __wrap_close(int fd) {
    const int r = __real_close(fd);
    ++g_closeAll;
    if (g_watchClose) { ++g_closeCalls; ++g_arms; g_arm = 1; }
    return r;
}
}

using industrial::FrameType;
using industrial::MultiReactorServer;

namespace {

std::atomic<int> g_closeHandlerCalls{0};

// closeCallbacksDropped exists only in the tree that absorbs the failure. Read
// it where it exists and report 0 where it does not, so one binary can show
// both sides.
template <typename S>
auto droppedCounter(const S& s, int) -> decltype(s.closeCallbacksDropped.load()) {
    return s.closeCallbacksDropped.load();
}
template <typename S>
uint64_t droppedCounter(const S&, long) { return 0; }

uint16_t pickPort() {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return 0;
    struct sockaddr_in sa{};
    sa.sin_family      = AF_INET;
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    sa.sin_port        = 0;
    if (::bind(fd, (struct sockaddr*)&sa, sizeof(sa)) < 0) { ::close(fd); return 0; }
    socklen_t sl = sizeof(sa);
    ::getsockname(fd, (struct sockaddr*)&sa, &sl);
    ::close(fd);
    return ntohs(sa.sin_port);
}

int connectTo(uint16_t port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    struct sockaddr_in sa{};
    sa.sin_family      = AF_INET;
    sa.sin_port        = htons(port);
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::connect(fd, (struct sockaddr*)&sa, sizeof(sa)) < 0) { ::close(fd); return -1; }
    return fd;
}

bool sendAll(int fd, const void* p, size_t n) {
    const uint8_t* b = static_cast<const uint8_t*>(p);
    size_t off = 0;
    while (off < n) {
        ssize_t k = ::send(fd, b + off, n - off, 0);
        if (k <= 0) {
            if (errno == EINTR) continue;
            return false;
        }
        off += static_cast<size_t>(k);
    }
    return true;
}

// Runs in the child. Returns 0 if the server survived the injected failure and
// close notifications still work afterwards.
int scenario() {
    MultiReactorServer::Config cfg;
    cfg.bindAddr      = "127.0.0.1";
    cfg.port          = pickPort();
    cfg.numReactors   = 1;
    cfg.tickMs        = 50;
    cfg.idleTimeoutMs = 60000;
    cfg.closeLingerMs = 1000;

    MultiReactorServer server(cfg);
    std::atomic<uint64_t> id{0};
    server.setMessageHandler([&](uint64_t cid, const uint8_t*, size_t) { id.store(cid); });
    server.setCloseHandler([](uint64_t, const std::string&, const char*) {
        g_closeHandlerCalls.fetch_add(1, std::memory_order_relaxed);
    });
    if (!server.start()) { std::printf("child: start failed\n"); return 2; }

    int fd = connectTo(cfg.port);
    if (fd < 0) { std::printf("child: connect failed\n"); server.stop(); return 2; }

    std::vector<uint8_t> f(industrial::FRAME_HEADER_SIZE + 2);
    industrial::encodeFrameInto(f.data(), FrameType::DATA, "hi", 2);
    (void)sendAll(fd, f.data(), f.size());
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // Arm, then close. The window has to cover closeLingerMs as well: an
    // application close parks until the peer's own FIN, so the descriptor is
    // closed a linger later, not when closeConnection() returns. The client
    // socket is held open and closed only after the window, so the close() that
    // arms the injection is the server's.
    g_watchClose = 1;
    server.closeConnection(id.load());
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
    g_watchClose = 0;
    ::close(fd);

    // Second connection, nothing injected: the close handler has to run for it,
    // which is how "absorbed" is told apart from "broken".
    int fd2 = connectTo(cfg.port);
    if (fd2 < 0) { std::printf("child: second connect failed\n"); server.stop(); return 2; }
    std::vector<uint8_t> f2(industrial::FRAME_HEADER_SIZE + 2);
    industrial::encodeFrameInto(f2.data(), FrameType::DATA, "hi", 2);
    (void)sendAll(fd2, f2.data(), f2.size());
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    server.closeConnection(id.load());
    std::this_thread::sleep_for(std::chrono::milliseconds(1500));
    ::close(fd2);

    const int calls = g_closeHandlerCalls.load();
    std::printf("child: survived; watched closes %d (armed %d, of %d total), "
                "injected new failures %d, "
                "closeCallbacksDropped %llu, closeHandler calls %d, closed %llu\n",
                g_closeCalls, g_arms, g_closeAll,
                g_newFailures,
                (unsigned long long)droppedCounter(server.stats(), 0),
                calls,
                (unsigned long long)server.stats().closed.load());
    // The point of the second connection: absorbing the failure must not be the
    // same thing as close notifications having stopped.
    std::printf("child: [%s] close callbacks still run for the connection "
                "after the injected one\n", calls >= 1 ? "PASS" : "FAIL");
    server.stop();
    return calls >= 1 ? 0 : 1;
}

}  // namespace

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);
    std::printf("teardownClosed(): one allocation failure where the close\n"
                "notification is queued. What matters is whether the process\n"
                "is still alive afterwards.\n\n");

    const pid_t pid = ::fork();
    if (pid == 0) _exit(scenario());

    int status = 0;
    ::waitpid(pid, &status, 0);
    if (WIFSIGNALED(status)) {
        std::printf("parent: child died from signal %d (%s)\n",
                    WTERMSIG(status),
                    WTERMSIG(status) == SIGABRT ? "SIGABRT: exception crossed noexcept"
                                                : "other");
        return 1;
    }
    std::printf("parent: child exited %d\n", WEXITSTATUS(status));
    return WEXITSTATUS(status) == 0 ? 0 : 1;
}
