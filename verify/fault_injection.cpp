// verify/fault_injection.cpp -- what a worker dying is supposed to look like.
//
// INDUSTRIAL_TCP_TESTING adds two switches so these paths can be exercised, and
// until now nothing exercised them: two switches with no consumer, in a
// directory whose only other check was compile-only. A switch nobody flips is no
// more a test than a commented-out assert.
//
// Both switches raise exactly the same question, which is what recovery means
// for a thread that left its loop through an exception rather than a request:
//
//   A. failWorkerForTest -- a reactor throwing out of loop() must be visible
//      (reactorFailures, isRunning() false), must not take the process with it,
//      and must leave a stoppable server behind.
//   B. failAcceptForTest -- same for acceptLoop(), plus: the listening socket it
//      owned must go away, or the port is held by a server that is not listening.
//   C. Injecting across repeated restarts -- anything that claims a failed worker
//      is recoverable has to survive being restarted after one died, because that
//      is the only shape "recoverable" can take that anyone would rely on.
//   D. closeLingerMs <= 0 must be refused at start(): doTick only sweeps a parked
//      close when linger > 0, so a non-positive value turns every peer that never
//      answers our FIN into a permanent fd + admission leak. Same for backlog <= 0:
//      listen() takes the value as given, and a zero-length accept queue looks
//      exactly like a server that randomly refuses connections under load.
//
// Build (Linux only), one line, so nothing here can be copied wrong:
//   g++ -std=c++17 -Wall -Wextra -pthread -DINDUSTRIAL_TCP_TESTING verify/fault_injection.cpp -o fault_injection
//
// The define is what unlocks the switches: without it this file does not compile,
// which is the intended failure mode -- a test that quietly stops testing is
// worse than one that refuses to build.
//
// Exit code 0 = all cases passed.
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <future>
#include <string>
#include <thread>
#include <vector>

#ifndef INDUSTRIAL_TCP_TESTING
#error "fault_injection.cpp requires -DINDUSTRIAL_TCP_TESTING; see the file header"
#endif

#include "tcp-server-mt.h"

using industrial::MultiReactorServer;

namespace {

int gFailures = 0;

void expect(bool ok, const char* what) {
    std::printf("  [%s] %s\n", ok ? "PASS" : "FAIL", what);
    if (!ok) ++gFailures;
}

// Runs fn elsewhere and gives it `ms` to finish.
//
// A recovery that deadlocks otherwise presents as a test that takes a long time,
// and nothing in the exit code distinguishes that from a slow machine. On timeout
// this leaves the process immediately rather than returning: the future from
// std::async blocks in its destructor until the task finishes, so the only way to
// report a hang is to not wait for it.
template <class F>
bool within(int ms, const char* what, F&& fn) {
    std::atomic<bool> done{false};
    std::thread t([&] { fn(); done.store(true); });
    t.detach();
    for (int i = 0; i < ms / 10; ++i) {
        if (done.load()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    std::printf("  [FAIL] %s did not return within %d ms\n", what, ms);
    std::fflush(stdout);
    std::_Exit(1);
}

bool waitFor(int ms, const std::function<bool()>& pred) {
    for (int i = 0; i < ms / 10; ++i) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return pred();
}

uint16_t pickPort() {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in sa{};
    sa.sin_family = AF_INET;
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    ::bind(fd, (struct sockaddr*)&sa, sizeof(sa));
    socklen_t len = sizeof(sa);
    ::getsockname(fd, (struct sockaddr*)&sa, &len);
    uint16_t port = ntohs(sa.sin_port);
    ::close(fd);
    return port;
}

// Anything still accepting here? Answers "was the listening socket actually
// released" without depending on how a subsequent bind would fail.
bool portIsListening(uint16_t port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return false;
    struct sockaddr_in sa{};
    sa.sin_family = AF_INET;
    sa.sin_port = htons(port);
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    bool ok = ::connect(fd, (struct sockaddr*)&sa, sizeof(sa)) == 0;
    if (ok) ::close(fd);
    ::close(fd);
    return ok;
}

}  // namespace

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);

    MultiReactorServer::Config base;
    base.bindAddr = "127.0.0.1";
    base.numReactors = 2;

    // ---- A: a reactor throwing out of its loop ------------------------------
    {
        std::printf("A. a reactor leaves its loop through an exception\n");
        MultiReactorServer::Config cfg = base;
        cfg.port = pickPort();
        MultiReactorServer server(cfg);
        expect(server.start(), "start() ok");
        const uint64_t before = server.stats().reactorFailures.load();

        server.failWorkerForTest.store(true);
        expect(waitFor(3000, [&] { return !server.isRunning(); }),
               "the server stops reporting itself as running");
        expect(server.stats().reactorFailures.load() > before,
               "reactorFailures counted it");
        expect(within(3000, "stop()", [&] { server.stop(); }),
               "stop() still completes");
    }

    // ---- B: the accept loop throwing ----------------------------------------
    {
        std::printf("B. the accept loop leaves through an exception\n");
        MultiReactorServer::Config cfg = base;
        cfg.port = pickPort();
        const uint16_t port = cfg.port;
        MultiReactorServer server(cfg);
        expect(server.start(), "start() ok");
        expect(portIsListening(port), "listening before the fault");
        const uint64_t before = server.stats().reactorFailures.load();

        server.failAcceptForTest.store(true);
        expect(waitFor(3000, [&] { return !server.isRunning(); }),
               "the server stops reporting itself as running");
        expect(server.stats().reactorFailures.load() > before,
               "reactorFailures counted it");
        expect(waitFor(3000, [&] { return !portIsListening(port); }),
               "the listening socket it owned was released");
        expect(within(3000, "stop()", [&] { server.stop(); }),
               "stop() still completes");
    }

    // ---- C: fails, restarts, fails again ------------------------------------
    {
        std::printf("C. 12 rounds of start -> fault -> stop -> start\n");
        MultiReactorServer::Config cfg = base;
        cfg.port = pickPort();
        // One reactor, so there is no question about which worker picked it up
        // and no round passes by targeting a worker that has already left.
        cfg.numReactors = 1;
        MultiReactorServer server(cfg);

        int usableAgain = 0;
        for (int round = 0; round < 12; ++round) {
            if (!server.start()) {
                std::printf("  [FAIL] round %d: start() refused after %d "
                            "recoveries\n", round, usableAgain);
                ++gFailures;
                break;
            }
            const uint64_t before = server.stats().reactorFailures.load();
            if (round % 2 == 0) server.failWorkerForTest.store(true);
            else                server.failAcceptForTest.store(true);
            if (!waitFor(3000, [&] { return !server.isRunning(); })) {
                std::printf("  [FAIL] round %d: the fault never took effect\n", round);
                ++gFailures;
                break;
            }
            if (server.stats().reactorFailures.load() <= before) {
                std::printf("  [FAIL] round %d: reactorFailures did not move\n", round);
                ++gFailures;
                break;
            }
            if (!within(3000, "stop()", [&] { server.stop(); })) break;
            ++usableAgain;
        }
        expect(usableAgain == 12, "every round left a server that could start again");
        std::printf("      completed %d of 12 rounds\n", usableAgain);
    }

    // ---- D: a configuration that cannot be honoured is refused --------------
    {
        std::printf("D. closeLingerMs <= 0 is refused at start()\n");
        MultiReactorServer::Config cfg = base;
        cfg.port = pickPort();

        cfg.closeLingerMs = 0;
        MultiReactorServer server0(cfg);
        expect(!server0.start(), "closeLingerMs=0 refused");
        expect(!server0.isRunning(), "a refused start leaves the server stopped");

        cfg.closeLingerMs = -1;
        MultiReactorServer serverNeg(cfg);
        expect(!serverNeg.start(), "closeLingerMs=-1 refused");

        cfg.closeLingerMs = 1;
        MultiReactorServer serverOk(cfg);
        expect(serverOk.start(), "closeLingerMs=1 still accepted");
        if (serverOk.isRunning()) serverOk.stop();

        // The one accept-side setting that silently degrades: Linux clamps
        // backlog only from above (somaxconn), so 0 goes through as a
        // zero-length queue.
        cfg.closeLingerMs = 5000;
        cfg.backlog = 0;
        MultiReactorServer serverB0(cfg);
        expect(!serverB0.start(), "backlog=0 refused");

        cfg.backlog = -1;
        MultiReactorServer serverBNeg(cfg);
        expect(!serverBNeg.start(), "backlog=-1 refused");

        cfg.backlog = 1;
        MultiReactorServer serverBOk(cfg);
        expect(serverBOk.start(), "backlog=1 still accepted");
        if (serverBOk.isRunning()) serverBOk.stop();
    }

    std::printf("%s (%d failure%s)\n", gFailures == 0 ? "ALL PASS" : "FAILURES",
                gFailures, gFailures == 1 ? "" : "s");
    return gFailures == 0 ? 0 : 1;
}
