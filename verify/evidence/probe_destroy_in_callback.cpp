// verify/evidence/probe_destroy_in_callback.cpp
//
// The header used to say, about CloseHandler and ConnStateHandler(false):
//
//   "Close/state-false callbacks are deferred: ... so they may drop the last
//    external owner of this server."
//
// That sentence invited this exact program: own the server through a shared_ptr,
// keep the last reference where the callbacks can reach it, and let the
// connection teardown release it. What actually happened was a SIGABRT with no
// explanation -- destructor -> stop() refuses to join the calling thread ->
// acceptThread_ still joinable -> std::thread::~thread -> terminate.
//
// Two things are now true and are what this probe checks:
//
//   A. stop() from the close callback survives (that was always fine).
//   B. dropping the last owner there still aborts -- the deferral moves the call
//      off the connection's stack, not off a thread the server owns -- but the
//      abort is now named on stderr instead of arriving as
//      "terminate called without an active exception".
//
// Both scenarios run in forked children: the question is whether the process
// survives, so the parent waits and prints the exit status, and captures each
// child's stderr into a file so that a child that dies mid-sentence still gets
// to say its piece.
//
// Build and run (Linux only, from the repo root):
//   g++ -std=c++17 -Wall -Wextra -pthread -O1 probe_destroy_in_callback.cpp
//   ./probe_destroy_in_callback

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cerrno>
#include <cstdio>
#include <csignal>
#include <cstring>
#include <memory>
#include <thread>
#include <vector>

#include "../../tcp-server-mt.h"

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

std::vector<uint8_t> frame(const void* p, size_t n) {
    std::vector<uint8_t> f(industrial::FRAME_HEADER_SIZE + n);
    industrial::encodeFrameInto(f.data(), industrial::FrameType::DATA, p, n);
    return f;
}

// Held by the callback, which is the only thing that can still reach it once the
// connection goes away.
std::shared_ptr<MultiReactorServer> g_server;
std::atomic<int> g_closeCallbacks{0};

// A child that aborts never returns to report anything, so its stderr goes to a
// file the parent can read afterwards. stderr is unbuffered, so the diagnostic
// reaches the file even when the process dies on the next line.
const char* const kErrPath = "/tmp/probe_destroy_in_callback.stderr";
const char* const kMarker  = "destroyed from inside its own callback";

// freopen rather than open/dup2: this file has to type-check against
// verify/stub/ too, and O_CREAT is not part of the shim. stdio re-pointing is
// enough -- stderr is unbuffered, so the write survives the abort that follows.
void redirectStderrToFile() {
    if (std::freopen(kErrPath, "w", stderr) == nullptr) {
        std::printf("child: could not capture stderr\n");
    }
}

// What the child left behind, and whether it says why it died.
void reportChildStderr() {
    FILE* f = std::fopen(kErrPath, "r");
    if (!f) { std::printf("parent: no stderr captured\n\n"); return; }
    char line[512];
    bool named = false;
    while (std::fgets(line, sizeof(line), f)) {
        size_t n = std::strlen(line);
        while (n > 0 && (line[n - 1] == '\n' || line[n - 1] == '\r')) line[--n] = '\0';
        if (n == 0) continue;
        std::printf("child stderr: %s\n", line);
        if (std::strstr(line, kMarker) != nullptr) named = true;
    }
    std::fclose(f);
    std::printf("parent: the abort is named on stderr: %s\n\n", named ? "yes" : "no");
}

// The documented flow for a callback: request the stop, let the controlling
// thread finish it. Only "drop the last owner here" is in question.
int childStopOnly(uint16_t port) {
    MultiReactorServer::Config cfg;
    cfg.bindAddr       = "127.0.0.1";
    cfg.port           = port;
    cfg.numReactors    = 1;
    cfg.tickMs         = 100;
    cfg.idleTimeoutMs  = 60000;
    cfg.closeLingerMs  = 500;

    auto server = std::make_shared<MultiReactorServer>(cfg);
    std::atomic<int> stops{0};
    server->setMessageHandler([](uint64_t, const uint8_t*, size_t) {});
    server->setCloseHandler([&](uint64_t, const std::string&, const char*) {
        if (stops.fetch_add(1) == 0) {
            std::printf("child: close callback; calling stop() as documented\n");
            std::fflush(stdout);
            server->stop();
            std::printf("child: stop() returned from the callback\n");
            std::fflush(stdout);
        }
    });
    if (!server->start()) { std::printf("child: start failed\n"); return 2; }

    int fd = connectTo(port);
    if (fd < 0) { std::printf("child: connect failed\n"); return 2; }
    auto f = frame("x", 1);
    sendAll(fd, f.data(), f.size());
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    server->closeConnection(1);
    std::this_thread::sleep_for(std::chrono::milliseconds(800));
    std::printf("child: stop() from a callback survived; close callbacks %d\n",
                stops.load());
    std::fflush(stdout);
    ::close(fd);
    return 0;
}

int child(uint16_t port) {
    MultiReactorServer::Config cfg;
    cfg.bindAddr       = "127.0.0.1";
    cfg.port           = port;
    cfg.numReactors    = 1;
    cfg.tickMs         = 100;
    cfg.idleTimeoutMs  = 60000;
    cfg.closeLingerMs  = 500;

    g_server = std::make_shared<MultiReactorServer>(cfg);
    g_server->setMessageHandler([](uint64_t, const uint8_t*, size_t) {});
    g_server->setCloseHandler([&](uint64_t, const std::string&, const char* reason) {
        g_closeCallbacks.fetch_add(1);
        std::printf("child: close callback (%s); dropping the last owner\n",
                    reason ? reason : "");
        std::fflush(stdout);
        // What the header used to advertise as supported, and now names as a
        // violation when it happens.
        g_server.reset();
        std::printf("child: reset returned; destructors have run\n");
        std::fflush(stdout);
    });

    if (!g_server->start()) { std::printf("child: start failed\n"); return 2; }

    int fd = connectTo(port);
    if (fd < 0) { std::printf("child: connect failed\n"); return 2; }
    auto f = frame("x", 1);
    if (!sendAll(fd, f.data(), f.size())) { std::printf("child: send failed\n"); return 2; }
    std::this_thread::sleep_for(std::chrono::milliseconds(150));

    g_server->closeConnection(1);
    // Give the reactor time to reach the deferred callback.
    std::this_thread::sleep_for(std::chrono::milliseconds(800));

    std::printf("child: survived; close callbacks %d\n", g_closeCallbacks.load());
    std::fflush(stdout);
    ::close(fd);
    g_server.reset();
    return 0;
}

}  // namespace

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);
    std::printf("dropping the last owner of the server from inside its own close\n"
                "callback -- what the header used to advertise as supported, and\n"
                "what it now names on the way out\n\n");

    for (int scenario = 0; scenario < 2; ++scenario) {
        std::printf("== %s ==\n", scenario == 0
                    ? "A. stop() from the close callback (the documented flow)"
                    : "B. dropping the last owner from the close callback");
        const uint16_t port = pickPort();
        pid_t pid = ::fork();
        if (pid == 0) {
            redirectStderrToFile();
            ::_exit(scenario == 0 ? childStopOnly(port) : child(port));
        }
        int status = 0;
        ::waitpid(pid, &status, 0);
        if (WIFSIGNALED(status)) {
            const int sig = WTERMSIG(status);
            // Not strsignal(): the host-side stub has no <string.h> declaration
            // for it, and this probe only ever needs to name one signal.
            const char* what = sig == SIGABRT ? "SIGABRT" : "signal";
            std::printf("parent: child died from signal %d (%s)\n", sig, what);
        } else {
            std::printf("parent: child exited %d\n", WEXITSTATUS(status));
        }
        reportChildStderr();
    }
    return 0;
}
