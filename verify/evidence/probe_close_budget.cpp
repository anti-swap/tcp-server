// verify/evidence/probe_close_budget.cpp
//
// A graceful close asked for from outside a handler goes to the reactor as a
// task, and the task wakes up inside drainInputThenClose(), which owes the peer
// the requests that were already buffered when it was asked for: one handleRead()
// moves at most 256 frames / 64 KiB, so serving a backlog can take more than one
// call, and every decision made in between would FIN over replies whose requests
// were already taken in. The number of those rounds is bounded by
// Conn::closeInputBytes, measured from what was in rbuf at the time:
//
//     if (served > 0)
//         c->closeInputBytes -= static_cast<int64_t>(served) * FRAME_HEADER_SIZE;
//
// A frame pays eight bytes however much it carried, so this is not a byte budget,
// it is a frame budget of backlog / 8 frames, and every one of those may be
// MAX_FRAME_PAYLOAD wide. A peer that keeps the pipe full therefore gets, say,
// ~8000 frames for one read buffer's worth of backlog -- and nothing else stops
// it either, because the close has not parked yet (no linger deadline exists) and
// every arrival refreshes lastRecvMs (no idle deadline applies).
//
// The other half is the loop itself. Before it looks at the task queue it runs
// readHead_ continuations, up to 64 of them, each of which may read another
// 64 KiB -- so ~4 MiB of input can be dispatched and answered between one look at
// the queue and the next, with the close request already sitting in it. That part
// is inherent to sharing one thread; it stops being harmless only when the rounds
// above keep handing the reactor another iteration to do it in.
//
// So the question is not "does the close finish" (it always does, shortly) but
// "does what the peer gets to say after the application hung up grow with how
// much it has to say". Two floods of the same connection, four times apart, are
// the whole measurement: if the second absorbs much the same as the first, the
// peer cannot extend it; if it absorbs most of what is new, it can.
//
// Attribution is by payload: frames sent before the request are filled with one
// byte and frames sent after it with another, so the handler can count exactly
// the frames whose bytes arrived after the request and whether their replies were
// admitted. Server counters cannot do this -- framesRecv also counts what is read
// and discarded during the linger, and bytesSent includes everything already owed
// when the application hung up, which legitimately goes out ahead of the FIN.
//
// The gate is the subject's own handler, which answers and then blocks inside its
// first dispatch -- the same trick probe_close_vs_fin uses. It fixes how much had
// been read, and how much served, at the moment the close was asked for.
//
// Build and run (Linux only, from the repo root):
//   g++ -std=c++17 -Wall -Wextra -pthread -O1
//       verify/evidence/probe_close_budget.cpp -o probe_close_budget
//   ./probe_close_budget
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "tcp-server-mt.h"

using industrial::FrameType;
using industrial::MultiReactorServer;

namespace {

// Payload per frame: large enough that eight bytes is nowhere near what a frame
// costs the budget, small enough that one read round moves several of them.
constexpr size_t kPayload = 8 * 1024;

// Sent before the close is asked for: enough to fill the read buffer and leave
// the socket holding the rest, so the backlog is unambiguous.
constexpr int kPreFrames = 16;

// The two floods. What distinguishes "bounded" from "extendable" is that the
// second is four times the first.
constexpr int kCapSmall = 1200;
constexpr int kCapLarge = 4 * kCapSmall;

constexpr int kDeadlineMs = 10000;

struct Gate {
    std::mutex              m;
    std::condition_variable cv;
    bool                    open    = false;
    std::atomic<bool>       entered{false};
};

struct Run {
    long pushed   = 0;   // frames sent after the request
    long handled  = 0;   // ...dispatched to the handler
    long accepted = 0;   // ...whose reply was admitted
    long refused  = 0;   // ...whose reply was refused
    long finMs    = -1;  // when the FIN reached the peer
};

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

std::vector<uint8_t> encode(size_t len, char fill) {
    std::string p(len, fill);
    std::vector<uint8_t> f(industrial::FRAME_HEADER_SIZE + p.size());
    industrial::encodeFrameInto(f.data(), FrameType::DATA, p.data(), p.size());
    return f;
}

bool sendAll(int fd, const std::vector<uint8_t>& b) {
    size_t off = 0;
    while (off < b.size()) {
        ssize_t k = ::send(fd, b.data() + off, b.size() - off, MSG_NOSIGNAL);
        if (k <= 0) return false;
        off += static_cast<size_t>(k);
    }
    return true;
}

Run runScenario(int cap) {
    Run out;

    MultiReactorServer::Config cfg;
    cfg.bindAddr         = "127.0.0.1";
    cfg.port             = pickPort();
    cfg.numReactors      = 1;
    cfg.tickMs           = 100;
    cfg.idleTimeoutMs    = 60000;
    cfg.closeLingerMs    = 3000;
    cfg.rbufCapacity     = industrial::FRAME_HEADER_SIZE + industrial::MAX_FRAME_PAYLOAD;
    cfg.wbufCapacity     = 8u * 1024 * 1024;
    cfg.maxPendingBytes  = 8u * 1024 * 1024;
    cfg.writeBudgetBytes = 8u * 1024 * 1024;

    Gate gate;
    std::atomic<long> handled{0}, accepted{0}, refused{0};

    MultiReactorServer server(cfg);
    server.setMessageHandler([&](uint64_t cid, const uint8_t* p, size_t len) {
        const bool isPost = len > 0 && p[0] == 'z';
        if (isPost) handled.fetch_add(1, std::memory_order_relaxed);
        const auto r = server.sendTo(cid, p, len);
        if (isPost) {
            if (r == industrial::SendResult::Ok)
                accepted.fetch_add(1, std::memory_order_relaxed);
            else
                refused.fetch_add(1, std::memory_order_relaxed);
        }
        if (!gate.entered.exchange(true, std::memory_order_acq_rel)) {
            std::unique_lock<std::mutex> lk(gate.m);
            gate.cv.wait(lk, [&] { return gate.open; });
        }
    });
    if (!server.start()) { std::printf("start failed\n"); return out; }

    const int fd = connectTo(cfg.port);
    if (fd < 0) { std::printf("connect failed\n"); server.stop(); return out; }

    uint64_t id = 0;
    const auto regDeadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(3000);
    while (std::chrono::steady_clock::now() < regDeadline) {
        auto ids = server.connectionIds();
        if (!ids.empty()) { id = ids.at(0); break; }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (id == 0) { std::printf("never registered\n"); ::close(fd); server.stop(); return out; }

    std::atomic<bool> finSeen{false};
    std::atomic<bool> stopWriter{false};
    std::atomic<long> pushed{0};

    // Reads everything the server sends, so nothing is held up by a full peer
    // buffer, and notices the FIN.
    std::thread reader([&] {
        std::vector<char> b(256 * 1024);
        for (;;) {
            ssize_t n = ::recv(fd, b.data(), b.size(), 0);
            if (n > 0) continue;
            if (n == 0) { finSeen.store(true); stopWriter.store(true); return; }
            if (errno == EINTR) continue;
            return;
        }
    });

    for (int i = 0; i < kPreFrames; ++i) sendAll(fd, encode(kPayload, 'a'));
    {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(3000);
        while (std::chrono::steady_clock::now() < deadline &&
               !gate.entered.load(std::memory_order_acquire))
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (!gate.entered.load(std::memory_order_acquire)) {
        std::printf("  the handler never ran: nothing was buffered\n");
        stopWriter.store(true);
        ::shutdown(fd, SHUT_RDWR);
        reader.join();
        ::close(fd);
        server.stop();
        return out;
    }

    const auto t0 = std::chrono::steady_clock::now();
    server.closeConnection(id);
    {
        std::lock_guard<std::mutex> lk(gate.m);
        gate.open = true;
    }
    gate.cv.notify_all();

    std::thread writer([&] {
        const auto deadline = std::chrono::steady_clock::now()
                            + std::chrono::milliseconds(kDeadlineMs);
        for (int i = 0; i < cap; ++i) {
            if (stopWriter.load(std::memory_order_acquire)) break;
            if (std::chrono::steady_clock::now() > deadline) break;
            if (!sendAll(fd, encode(kPayload, 'z'))) break;
            pushed.fetch_add(1, std::memory_order_relaxed);
        }
    });

    const auto finDeadline = std::chrono::steady_clock::now()
                           + std::chrono::milliseconds(kDeadlineMs + 2000);
    while (std::chrono::steady_clock::now() < finDeadline &&
           !finSeen.load(std::memory_order_acquire))
        std::this_thread::sleep_for(std::chrono::milliseconds(1));

    out.finMs = finSeen.load(std::memory_order_acquire)
        ? static_cast<long>(std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::steady_clock::now() - t0).count())
        : -1;

    stopWriter.store(true);
    writer.join();
    ::shutdown(fd, SHUT_RDWR);
    reader.join();
    ::close(fd);
    server.stop();

    out.pushed   = pushed.load();
    out.handled  = handled.load();
    out.accepted = accepted.load();
    out.refused  = refused.load();
    return out;
}

}  // namespace

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);

    const Run small = runScenario(kCapSmall);
    const Run large = runScenario(kCapLarge);

    std::printf("same connection, same 16-frame backlog, close asked for from the\n"
                "application side; then the peer floods %d / %d frames.\n\n",
                kCapSmall, kCapLarge);

    auto report = [](const char* what, const Run& r) {
        std::printf("  %s flood: pushed %ld, handler called for %ld,\n"
                    "        replies admitted %ld, refused %ld; FIN after %ld ms\n",
                    what, r.pushed, r.handled, r.accepted, r.refused, r.finMs);
    };
    report("small", small);
    report("large", large);
    std::printf("\n");

    const bool bothFinished = small.finMs >= 0 && large.finMs >= 0;
    // Four times the flood buying materially more service is the whole claim
    // being tested. What is honestly still available afterwards is one loop
    // iteration's worth of read continuation (64 calls of 64 KiB -- see loop()),
    // which the small flood already contains, so the slop allows that much again.
    const long ceiling = small.accepted + 64;
    const bool independent = large.accepted <= ceiling;

    std::printf("  [%s] the FIN went out in both runs\n", bothFinished ? "PASS" : "FAIL");
    std::printf("  [%s] four times the flood does not buy four times the service:\n"
                "        %ld admitted against %ld, ceiling %ld\n",
                independent ? "PASS" : "FAIL", large.accepted, small.accepted, ceiling);

    const bool ok = bothFinished && independent && small.accepted > 0;
    return ok ? 0 : 1;
}
