// verify/evidence/probe_rejected_conn.cpp
//
// submitNewConnection()'s registration runs inside a queued task:
//
//     fdIndex_[c->fd] = c.get();          // may throw bad_alloc
//     conns_[id]      = c;                // may throw bad_alloc
//     if (!server_->onConnectionOpened(id, c)) { ... }
//
// The rollback was unregisterConn() alone, which erases fdIndex_, drops the
// epoll registration, closes the descriptor and sets fd to -1 -- and leaves the
// conns_ entry behind. What is left is a Conn with no descriptor that is
// neither closed nor closing.
//
// Where it ends up is worth stating precisely, because the obvious guess is
// wrong: doTick()'s sweep does not reap it. The idle branch parks a close that
// tryBeginWriteShutdown() refuses to start (fd < 0), which leaves `closing`
// set; the linger branch then requires `c->fd >= 0` before it force-expires
// anything, so a ghost satisfies neither. It survives every tick and is torn
// down by cleanup() when the reactor shuts down -- as "server shutdown",
// counted as closed and aborted, for a connection that was never accepted.
//
// So the question this probe asks is: does a connection that was never accepted
// nevertheless get counted as closed?
//
//   accepted == closed  -- the refused connection left nothing behind
//   closed > accepted   -- something that was never a connection was closed
//
// The injection is placed by counting allocations from the epoll_ctl(ADD) that
// registers the descriptor, which is the last thing before the two inserts and
// is itself wrappable:
//
//   --wrap=epoll_ctl  arms a countdown on the first ADD for a connection
//   --wrap=_Znwm      throws on the Nth allocation after the arm
//
// The maps are warmed by a first connection that is not injected, so neither
// insert can be the one that grows a bucket array and shift the count.
//
// Which N is the right one is measured, not assumed -- INJECT_AFTER_ADD picks
// it, and the three nearby values say why:
//
//   N=1  fdIndex_ insert fails      -> rejected 1, nothing left behind
//   N=2  reactor conns_ insert fails-> rejected 1, nothing left behind
//   N=3  onConnectionOpened's map fails -> rejected 1, and the ghost (this one)
//   N=4  misses registration entirely -> accepted 1, the injection did not land
//
// Failing an insert that would have created the entry cannot produce a ghost;
// only a failure *after* the entry exists can. N=3 is that case.
//
// Build and run (Linux only, from the repo root) -- one line, because a
// trailing backslash inside a // comment swallows the line after it:
//   g++ -std=c++17 -Wall -Wextra -pthread -O1 -Wl,--wrap=epoll_ctl -Wl,--wrap=_Znwm verify/evidence/probe_rejected_conn.cpp -o probe_rejected_conn
//   ./probe_rejected_conn
//
// It compiles against the tree before the fix and the one after it: both
// counters it reads have always existed. That is the point -- one binary has to
// be able to show the red side and the green side.
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <new>
#include <thread>
#include <vector>

#include "tcp-server-mt.h"

// Plain globals: read from inside the linker wrappers, which must have C
// linkage and therefore cannot capture anything.
int g_watchAdd  = 0;    // arm on the next EPOLL_CTL_ADD
int g_countdown = 0;    // throw on the Nth allocation after arming
int g_newFails  = 0;    // allocations that actually threw
int g_arms      = 0;    // times the injection was armed

// Which allocation after the ADD carries the failure. Read from the
// environment (INJECT_AFTER_ADD) so one binary can be pointed at each step of
// the registration in turn: how many allocations libstdc++ spends on a node
// insert is not something to hard-code blind.
int g_countdownAt = 3;   // see the table above; INJECT_AFTER_ADD overrides it

extern "C" {
void* __real__Znwm(size_t);
void* __wrap__Znwm(size_t n) {
    if (g_countdown > 0) {
        --g_countdown;
        if (g_countdown == 0) { ++g_newFails; throw std::bad_alloc(); }
    }
    return __real__Znwm(n);
}
int __real_epoll_ctl(int, int, int, struct epoll_event*);
int __wrap_epoll_ctl(int epfd, int op, int fd, struct epoll_event* ev) {
    const int r = __real_epoll_ctl(epfd, op, fd, ev);
    // Arm after the call, not before: the ADD itself must succeed, and nothing
    // inside it may consume the countdown.
    if (r == 0 && op == EPOLL_CTL_ADD && g_watchAdd) {
        g_watchAdd = 0;
        ++g_arms;
        // Which allocation after the ADD to fail. It has to be late enough that
        // the reactor's own conns_ insert has already succeeded: failing an
        // earlier one leaves nothing behind, because an insert that throws does
        // not insert. The ghost is what survives when conns_[id] is there and
        // the step after it is what failed.
        g_countdown = g_countdownAt;
    }
    return r;
}
}

using industrial::FrameType;
using industrial::MultiReactorServer;

namespace {

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

// Runs in a forked child: what is being measured is whether the reactor is
// still there afterwards, and a child that dies cannot report on itself.
int scenario() {
    MultiReactorServer::Config cfg;
    cfg.bindAddr      = "127.0.0.1";
    cfg.port          = pickPort();
    cfg.numReactors   = 1;
    cfg.tickMs        = 50;
    // Short on purpose: a ghost is reaped by the idle sweep plus the linger, so
    // the window below only has to outlast those two to see it counted.
    cfg.idleTimeoutMs = 400;
    cfg.closeLingerMs = 200;

    MultiReactorServer server(cfg);
    server.setMessageHandler([](uint64_t, const uint8_t*, size_t) {});
    if (!server.start()) { std::printf("child: start failed\n"); return 2; }

    // Warm connection: it fills both maps' bucket arrays, so neither insert in
    // the injected connection can be the one that grows them.
    int warm = connectTo(cfg.port);
    if (warm < 0) { std::printf("child: warm connect failed\n"); server.stop(); return 2; }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    // And it must be gone before the baselines: it is a connection that really
    // was accepted and really will be closed, and idleTimeoutMs here is short
    // enough that it would otherwise be reaped in the measured window and read
    // as the ghost this probe is looking for.
    ::close(warm);
    std::this_thread::sleep_for(std::chrono::milliseconds(600));

    const unsigned long long accepted0 = server.stats().accepted.load();
    const unsigned long long rejected0 = server.stats().rejected.load();
    const unsigned long long closed0   = server.stats().closed.load();
    const unsigned long long aborted0  = server.stats().aborted.load();

    g_watchAdd = 1;
    int fd = connectTo(cfg.port);
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    g_watchAdd = 0;
    g_countdown = 0;

    // Long enough for the idle sweep and the linger to have run twice over.
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));

    const unsigned long long accepted = server.stats().accepted.load() - accepted0;
    const unsigned long long rejected = server.stats().rejected.load() - rejected0;
    const unsigned long long closed   = server.stats().closed.load()   - closed0;
    const unsigned long long aborted  = server.stats().aborted.load()  - aborted0;

    std::printf("child: armed %d, injected allocation failures %d\n", g_arms, g_newFails);
    std::printf("child: while running -- accepted %llu, rejected %llu, "
                "closed %llu, aborted %llu\n",
                accepted, rejected, closed, aborted);

    if (fd >= 0) ::close(fd);
    server.stop();

    // After stop() is where a ghost shows up, and that is itself the finding:
    // doTick's sweep will not reap it -- the linger branch requires fd >= 0, and
    // an fd of -1 never satisfies it -- so it survives every tick and is torn
    // down with whatever is left when the reactor shuts down. A connection that
    // was never accepted is closed and aborted there, one shutdown later.
    const unsigned long long closedEnd  = server.stats().closed.load()  - closed0;
    const unsigned long long abortedEnd = server.stats().aborted.load() - aborted0;
    std::printf("child: after stop()  -- accepted %llu, rejected %llu, "
                "closed %llu, aborted %llu\n",
                accepted, rejected, closedEnd, abortedEnd);

    // The injection has to have landed where it was aimed, or everything below
    // reads as a pass without having tested anything.
    if (g_newFails != 1) { std::printf("child: [INCONCLUSIVE] the injection did not land once\n"); return 3; }
    if (rejected != 1) { std::printf("child: [INCONCLUSIVE] rejected is not 1\n"); return 3; }

    const bool ok = (closedEnd <= accepted);
    std::printf("child: [%s] a connection that was never accepted is never closed\n",
                ok ? "PASS" : "FAIL");
    return ok ? 0 : 1;
}

}  // namespace

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);
    if (const char* e = std::getenv("INJECT_AFTER_ADD")) g_countdownAt = std::atoi(e);
    std::printf("submitNewConnection(): one allocation failure while registering a\n"
                "connection. Whether the refused connection leaves a ghost behind\n"
                "is measured as closed vs accepted.\n\n");

    const pid_t pid = ::fork();
    if (pid == 0) ::_exit(scenario());

    int status = 0;
    ::waitpid(pid, &status, 0);
    if (WIFSIGNALED(status)) {
        std::printf("parent: child died from signal %d (%s)\n", WTERMSIG(status),
                    WTERMSIG(status) == SIGABRT ? "SIGABRT: exception crossed noexcept"
                                                : "other");
        return 1;
    }
    std::printf("parent: child exited %d\n", WEXITSTATUS(status));
    return WEXITSTATUS(status) == 0 ? 0 : 1;
}
