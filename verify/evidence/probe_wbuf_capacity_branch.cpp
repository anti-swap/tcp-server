// verify/evidence/probe_wbuf_capacity_branch.cpp
//
// submitSend() has two refusal branches and they do not agree on what a refusal
// is worth:
//
//   - "already closing / closed": subtracts the reservation, and counts BOTH
//     bytesDropped (how much the peer never received) and queueDrops (how many
//     replies never went out). The comment above it insists on both, because one
//     event has to be answerable as either question.
//   - "wbuf has no room for this whole frame": subtracts the reservation and
//     counts queueDrops only. bytesDropped never moves.
//
// Run against the instrumented header (a fprintf on the second branch) to settle
// whether the disagreement is live or theoretical. The reasoning that says it is
// theoretical: start() clamps maxPendingBytes to wbufCapacity, a frame's
// reservation stays in pendingBytes until it is written into wbuf, and wbuf's
// contents are a subset of pendingBytes -- so at the moment the task runs,
// pendingBytes >= wbuf.readable() + frame.size(), which makes
// wbuf.writable() >= frame.size() unavoidable.
//
// This presses on it from every side: many concurrent producers (so the check in
// sendTo and the write in submitSend are far apart in time), a peer that reads
// slowly (so the drain keeps changing wbuf under them), mixed frame sizes, and a
// connection that is being closed while its replies are still in flight.
//
// Build and run (Linux only, from the repo root):
//   g++ -std=c++17 -Wall -Wextra -pthread -O1
//       verify/evidence/probe_wbuf_capacity_branch.cpp -o probe_wbuf_capacity
//   ./probe_wbuf_capacity 2>&1 | grep -c PROBE
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <thread>
#include <vector>

#include "tcp-server-mt.h"

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
    cfg.bindAddr        = "127.0.0.1";
    cfg.port            = pickPort();
    cfg.numReactors     = 1;
    cfg.tickMs          = 20;
    cfg.idleTimeoutMs   = 600000;
    cfg.closeLingerMs   = 5000;
    cfg.wbufCapacity    = 64u * 1024;      // small on purpose
    cfg.maxPendingBytes = 64u * 1024;      // clamped to wbufCapacity anyway
    cfg.writeBudgetBytes = 4 * 1024;       // slow drain: wbuf stays near full

    MultiReactorServer server(cfg);
    std::atomic<long> ok{0}, refused{0};
    std::atomic<uint64_t> connId{0};
    server.setMessageHandler([&](uint64_t id, const uint8_t*, size_t) {
        connId.store(id);
        (void)id;
    });
    if (!server.start()) { std::printf("start failed\n"); return 1; }

    int fd = connectTo(cfg.port);
    if (fd < 0) { std::printf("connect failed\n"); server.stop(); return 1; }

    // One request so the connection id is known to the producers.
    {
        std::vector<uint8_t> f(industrial::FRAME_HEADER_SIZE + 4);
        industrial::encodeFrameInto(f.data(), industrial::FrameType::DATA, "hi", 2);
        (void)::send(fd, f.data(), f.size(), 0);
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    const uint64_t id = connId.load();
    std::atomic<bool> go{true};

    // Eight producers, mixed frame sizes, hammering one connection whose peer
    // reads a little now and then.
    std::vector<std::thread> producers;
    for (int t = 0; t < 8; ++t) {
        producers.emplace_back([&, t] {
            std::vector<uint8_t> payload(static_cast<size_t>(4 + t) * 1024, 'p');
            while (go.load()) {
                auto r = server.sendTo(id, payload.data(), payload.size());
                if (r == industrial::SendResult::Ok) ok.fetch_add(1);
                else refused.fetch_add(1);
                std::this_thread::sleep_for(std::chrono::microseconds(200));
            }
        });
    }

    // A reader that takes a little, slowly, so wbuf keeps refilling.
    std::vector<uint8_t> sink(64 * 1024);
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (std::chrono::steady_clock::now() < deadline) {
        ssize_t r = ::recv(fd, sink.data(), sink.size(), MSG_DONTWAIT);
        if (r == 0) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    // And a close while replies are still in flight: the one path that refuses
    // submissions for a different reason, to make sure the output distinguishes
    // the two branches rather than just being quiet.
    server.closeConnection(id);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    go.store(false);
    for (auto& th : producers) th.join();

    std::printf("sendTo Ok %ld refused %ld | queueDrops %llu bytesDropped %llu "
                "bytesSent %llu framesSent %llu\n",
                ok.load(), refused.load(),
                (unsigned long long)server.stats().queueDrops.load(),
                (unsigned long long)server.stats().bytesDropped.load(),
                (unsigned long long)server.stats().bytesSent.load(),
                (unsigned long long)server.stats().framesSent.load());
    std::printf("(any 'PROBE: wbuf-capacity branch hit' lines on stderr are the "
                "branch firing)\n");

    ::close(fd);
    server.stop();
    return 0;
}
