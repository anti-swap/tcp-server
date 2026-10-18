// verify/evidence/probe_state_throw.cpp
//
// submitNewConnection()'s registration task ends like this:
//
//     server_->statsRef().accepted.fetch_add(1, relaxed);
//     auto& sh = server_->stateHandlerRef();
//     if (sh) sh(id, c->peer, true);
//   } catch (...) {
//     server_->statsRef().rejected.fetch_add(1, relaxed);
//   }
//
// The connection has just been put in both maps and handed its epoll
// registration; nothing anywhere is going to undo that because the application's
// ConnStateHandler threw. It stays connected, it stays in connectionCount(), it
// can be sent to, and it will eventually be counted in closed. But the counters
// then say two contradictory things at once: accepted says 1 (it was), rejected
// says 1 (it was not).
//
// The count that is wrong is not the interesting part -- it is which one to
// believe. `rejected` is the counter an operator reads to find clients being
// turned away, and everything else in this file is careful to make accepted -
// closed agree with reality (that is why accepted moved inside this task in the
// first place). A throwing callback is none of those things: the connection is
// up. Nothing was refused.
//
// This probe says so from the outside, which is the only place the difference is
// visible: one connection accepted appears in accepted and in rejected at the
// same time.
//
// Build and run (Linux only, from the repo root):
//   g++ -std=c++17 -Wall -Wextra -pthread -O1
//       verify/evidence/probe_state_throw.cpp -o probe_state_throw
//   ./probe_state_throw
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>

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

}  // namespace

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);

    MultiReactorServer::Config cfg;
    cfg.bindAddr    = "127.0.0.1";
    cfg.port        = pickPort();
    cfg.numReactors = 1;

    MultiReactorServer server(cfg);
    std::atomic<int> opens{0};
    // Exactly one throw, for exactly one open: enough to count it, not enough to
    // make the counts depend on how many times it fired.
    server.setConnStateHandler([&](uint64_t, const std::string&, bool up) {
        if (up) opens.fetch_add(1, std::memory_order_relaxed);
        // Only the open transition. Throwing out of the close-side callbacks is
        // a different story: they run inside a noexcept function with nothing
        // around them, so the contract ("handlers must not throw") is enforced
        // by std::terminate, and there is no counting left to misread.
        if (!up) return;
        throw std::runtime_error("application state handler");
    });
    server.setMessageHandler([](uint64_t, const uint8_t*, size_t) {});

    if (!server.start()) { std::printf("start failed\n"); return 2; }

    const int fd = connectTo(cfg.port);
    if (fd < 0) { std::printf("connect failed\n"); server.stop(); return 2; }

    size_t live = 0;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(2000);
    while (std::chrono::steady_clock::now() < deadline) {
        live = server.connectionCount();
        if (live > 0 && opens.load(std::memory_order_relaxed) > 0) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    const unsigned long long accepted = server.stats().accepted.load();
    const unsigned long long rejected = server.stats().rejected.load();

    ::close(fd);
    unsigned long long closed = 0;
    const auto closeDeadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(2000);
    while (std::chrono::steady_clock::now() < closeDeadline) {
        closed = server.stats().closed.load();
        if (closed > 0) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    server.stop();

    std::printf("one connection whose ConnStateHandler throws on open.\n\n");
    std::printf("  the callback ran for %d open(s)\n", opens.load(std::memory_order_relaxed));
    std::printf("  connectionCount() = %zu -- %s\n", live,
                live ? "it is connected" : "nothing stayed connected");
    std::printf("  accepted = %llu, rejected = %llu, closed = %llu\n\n",
                accepted, rejected, closed);

    // What should be true of one connection that was connected once and then
    // went away, whether or not someone's callback threw: it is counted in
    // accepted and in closed, and nowhere else.
    const bool oneAccepted = accepted == 1;
    const bool notAlsoRejected = rejected == 0;
    const bool stillCountedOnceClosed = closed == 1;

    std::printf("  [%s] accepted counts it: %llu\n", oneAccepted ? "PASS" : "FAIL", accepted);
    std::printf("  [%s] rejected does not count it too: %llu\n",
                notAlsoRejected ? "PASS" : "FAIL", rejected);
    std::printf("  [%s] and it still closes once: closed %llu\n",
                stillCountedOnceClosed ? "PASS" : "FAIL", closed);

    const bool ok = oneAccepted && notAlsoRejected && stillCountedOnceClosed;
    return ok ? 0 : 1;
}
