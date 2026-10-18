// verify/evidence/probe_close_vs_fin.cpp
//
// drainInputThenClose() decides whether to park the close with:
//
//     if (served == 0 || c->waitingOnReplies) { parkClose(c, "requested"); }
//
// The second half of that condition reads `waitingOnReplies` as "the one queue
// round this close was allowed to defer has already been spent, so park now".
// But finishPeerClosed() sets the same flag for a different reason: it has just
// seen the peer's FIN, found replies still reserved, and queued its own resume
// so those replies reach wbuf before anything is FIN'd. When that happens inside
// the handleRead() this function called, the deferral has NOT been spent -- the
// task that would spend it is still in the queue, behind the very replies it was
// deferring for.
//
// Parking on that flag is therefore one round too early: `closing` is set
// (parkClose does it first thing), the FIN goes out immediately because wbuf is
// still empty, and every reply queued by the frames just dispatched is refused
// by submitSend() -- counted into bytesDropped, for a request that was read,
// handed to the handler, and answered into nothing.
//
// The window is narrow but not artificial: it is "the peer sent its request and
// its FIN while the application was asking to hang up". Both orderings of the
// same three events are one event apart:
//
//   request read by the epoll handler  -> finishPeerClosed defers -> reply sent
//   request read by the close task     -> parked at once          -> reply lost
//
// Which one happens depends on whether the reactor read the request before or
// after the close task ran. This probe removes the coin flip: it makes the
// reactor busy inside an unrelated connection's handler, so the subject's bytes
// are guaranteed to still be in the kernel buffer when the close task runs --
// with a matched control case (same setup, no FIN) to show the difference is the
// FIN, not the gate.
//
// Build and run (Linux only, from the repo root):
//   g++ -std=c++17 -Wall -Wextra -pthread -O1
//       verify/evidence/probe_close_vs_fin.cpp -o probe_close_vs_fin
//   ./probe_close_vs_fin
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
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

constexpr size_t kPayload = 32;
constexpr size_t kFrame   = industrial::FRAME_HEADER_SIZE + kPayload;

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

std::vector<uint8_t> encode(FrameType type, const std::string& p) {
    std::vector<uint8_t> f(industrial::FRAME_HEADER_SIZE + p.size());
    industrial::encodeFrameInto(f.data(), type, p.data(), p.size());
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

// The reactor is single-threaded here on purpose: the gate handler must be able
// to stop it dead, and with one reactor there is no other worker to sneak the
// subject's bytes in through.
struct Gate {
    std::mutex              m;
    std::condition_variable cv;
    bool                    open    = false;
    std::atomic<bool>       entered{false};
};

struct Outcome {
    long               answeredFrames = 0;
    long               answeredBytes  = 0;
    long               echoesQueued   = 0;
    bool               closeAccepted  = false;
    unsigned long long bytesDropped   = 0;
    unsigned long long queueDrops     = 0;
    unsigned long long framesSent     = 0;
    unsigned long long framesRecv     = 0;
    unsigned long long closed         = 0;
    unsigned long long aborted        = 0;
    bool               truncated      = false;   // read ended without a FIN
};

// peerFin: does the subject half-close right after its request? That single flag
// is the whole difference between "the close has to wait one round for the
// reply" and "the close called it a day".
Outcome run(bool peerFin) {
    Outcome out;

    MultiReactorServer::Config cfg;
    cfg.bindAddr        = "127.0.0.1";
    cfg.port            = pickPort();
    cfg.numReactors     = 1;
    cfg.closeLingerMs   = 3000;
    cfg.idleTimeoutMs   = 60000;
    cfg.wbufCapacity    = 1u * 1024 * 1024;
    cfg.maxPendingBytes = 1u * 1024 * 1024;

    Gate gate;
    std::atomic<uint64_t> subjectId{0};
    std::atomic<uint64_t> gateId{0};
    std::atomic<long>     echoesQueued{0};

    MultiReactorServer server(cfg);
    server.setMessageHandler([&](uint64_t id, const uint8_t* p, size_t len) {
        if (id == gateId.load(std::memory_order_acquire)) {
            // Hold the reactor here. Everything the probe does from now on
            // happens while no event is being dispatched.
            gate.entered.store(true, std::memory_order_release);
            std::unique_lock<std::mutex> lk(gate.m);
            gate.cv.wait(lk, [&] { return gate.open; });
            return;
        }
        if (id == subjectId.load(std::memory_order_acquire)) {
            echoesQueued.fetch_add(1, std::memory_order_relaxed);
            server.sendTo(id, p, len);          // echo: the answer this probe counts
        }
    });

    if (!server.start()) {
        std::printf("  start failed\n");
        return out;
    }

    auto waitFor = [](auto pred, int ms) {
        const auto deadline = std::chrono::steady_clock::now()
                            + std::chrono::milliseconds(ms);
        while (std::chrono::steady_clock::now() < deadline) {
            if (pred()) return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return pred();
    };

    // 1. The subject exists first, so its id is knowable before anything blocks.
    const int sfd = connectTo(cfg.port);
    if (sfd < 0) { std::printf("  subject connect failed\n"); server.stop(); return out; }
    if (!waitFor([&] { return server.connectionCount() == 1; }, 2000)) {
        std::printf("  subject never registered\n");
        ::close(sfd); server.stop(); return out;
    }
    subjectId.store(server.connectionIds().at(0), std::memory_order_release);

    // 2. Then the gate connection, whose handler will stop the reactor.
    const int gfd = connectTo(cfg.port);
    if (gfd < 0) {
        std::printf("  gate connect failed\n");
        ::close(sfd); server.stop(); return out;
    }
    if (!waitFor([&] { return server.connectionCount() == 2; }, 2000)) {
        std::printf("  gate never registered\n");
        ::close(gfd); ::close(sfd); server.stop(); return out;
    }
    for (uint64_t id : server.connectionIds())
        if (id != subjectId.load()) gateId.store(id, std::memory_order_release);

    // 3. Block the reactor inside the gate's handler.
    if (!sendAll(gfd, encode(FrameType::DATA, std::string(kPayload, 'g'))) ||
        !waitFor([&] { return gate.entered.load(std::memory_order_acquire); }, 2000)) {
        std::printf("  gate handler never entered\n");
        ::close(gfd); ::close(sfd); server.stop(); return out;
    }

    // 4. Now the subject's request, and optionally its FIN. Nothing reads them:
    //    the only thread that could is blocked three lines up.
    if (!sendAll(sfd, encode(FrameType::DATA, std::string(kPayload, 'q')))) {
        std::printf("  subject send failed\n");
        ::close(gfd); ::close(sfd); server.stop(); return out;
    }
    if (peerFin) ::shutdown(sfd, SHUT_WR);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // 5. The application asks to hang up while that input is still unread.
    out.closeAccepted = server.closeConnection(subjectId.load(std::memory_order_acquire));
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    // 6. Release the reactor. Task order from here decides the outcome, and the
    //    queue holds exactly one task: the close.
    {
        std::lock_guard<std::mutex> lk(gate.m);
        gate.open = true;
    }
    gate.cv.notify_all();

    // 7. Read whatever the subject was answered, to EOF.
    struct timeval tv{};
    tv.tv_sec  = 2;
    tv.tv_usec = 0;
    ::setsockopt(sfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    std::vector<uint8_t> in;
    uint8_t buf[4096];
    for (;;) {
        ssize_t k = ::recv(sfd, buf, sizeof(buf), 0);
        if (k > 0) { in.insert(in.end(), buf, buf + k); continue; }
        if (k == 0) break;                       // FIN: the answer is complete
        out.truncated = true;                    // timeout / reset
        break;
    }
    out.answeredBytes  = static_cast<long>(in.size());
    out.answeredFrames = static_cast<long>(in.size() / kFrame);
    out.echoesQueued   = echoesQueued.load();

    // The FIN reaches this probe before the reactor has finished accounting for
    // what it refused: the echo task is still in the queue when parkClose's
    // shutdown() lands on the wire. Sampling the counters here without waiting
    // reads a moment that is not the one the question is about. The connection
    // is done once `closed` moves, and every refusal this close caused is
    // counted before that (the reply task was queued ahead of the teardown).
    waitFor([&] { return server.stats().closed.load() >= 1; }, 2000);

    const auto& st = server.stats();
    out.bytesDropped = st.bytesDropped.load();
    out.queueDrops   = st.queueDrops.load();
    out.framesSent   = st.framesSent.load();
    out.framesRecv   = st.framesRecv.load();
    out.closed       = st.closed.load();
    out.aborted      = st.aborted.load();

    ::close(gfd);
    ::close(sfd);
    server.stop();
    return out;
}

// The fix makes drainInputThenClose() hand the close back whenever the round is
// already owed to a peer-close deferral -- and that branch is taken with
// `served == 0` too, which is the ordinary ordering: the reactor reads the
// request and the FIN itself, defers, and only then does the application ask to
// hang up. Before the fix that call parked; now it returns without parking, and
// the close has to end in the task that owns the round anyway.
//
// This is the guard on that: whatever the close returns, the reply still reaches
// the peer and the connection still ends. The interesting count is `accepted` --
// the iterations where closeConnection() landed while the connection was still
// open, which are the ones that actually ran the changed branch.
struct LateRun {
    int               iterations   = 0;
    int               accepted      = 0;
    int               answered      = 0;
    unsigned long long bytesDropped = 0;
    unsigned long long queueDrops   = 0;
};

LateRun runLate(int repeats) {
    LateRun agg;

    for (int i = 0; i < repeats; ++i) {
        MultiReactorServer::Config cfg;
        cfg.bindAddr        = "127.0.0.1";
        cfg.port            = pickPort();
        cfg.numReactors     = 1;
        cfg.closeLingerMs   = 3000;
        cfg.idleTimeoutMs   = 60000;
        cfg.wbufCapacity    = 1u * 1024 * 1024;
        cfg.maxPendingBytes = 1u * 1024 * 1024;

        Gate gate;
        std::atomic<uint64_t> id{0};
        MultiReactorServer server(cfg);
        // The gate is the subject's own handler this time: it answers, and then
        // holds the reactor. The close is asked for while it is being held, so
        // by the time the reactor runs the close task it has already read both
        // the request and the FIN -- the deferral for those replies is owed to
        // peer-close, and the close task arrives to find it that way.
        server.setMessageHandler([&](uint64_t cid, const uint8_t* p, size_t len) {
            server.sendTo(cid, p, len);
            gate.entered.store(true, std::memory_order_release);
            std::unique_lock<std::mutex> lk(gate.m);
            gate.cv.wait(lk, [&] { return gate.open; });
        });
        if (!server.start()) { std::printf("  late start failed\n"); return agg; }

        const int fd = connectTo(cfg.port);
        if (fd < 0) { std::printf("  late connect failed\n"); server.stop(); return agg; }
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(2000);
        while (std::chrono::steady_clock::now() < deadline && server.connectionCount() == 0)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        if (server.connectionCount() == 0) {
            std::printf("  late connection never registered\n");
            ::close(fd); server.stop(); return agg;
        }
        id.store(server.connectionIds().at(0), std::memory_order_release);

        if (!sendAll(fd, encode(FrameType::DATA, std::string(kPayload, 'q')))) {
            ::close(fd); server.stop(); return agg;
        }
        ::shutdown(fd, SHUT_WR);

        // The request has been dispatched and its answer queued; the FIN is
        // still unread. Ask for the close now, from outside the reactor.
        const auto gDeadline = std::chrono::steady_clock::now()
                             + std::chrono::milliseconds(2000);
        while (std::chrono::steady_clock::now() < gDeadline &&
               !gate.entered.load(std::memory_order_acquire))
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        if (!gate.entered.load(std::memory_order_acquire)) {
            std::printf("  late handler never entered\n");
            ::close(fd); server.stop(); return agg;
        }

        ++agg.iterations;
        if (server.closeConnection(id.load(std::memory_order_acquire))) ++agg.accepted;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        {
            std::lock_guard<std::mutex> lk(gate.m);
            gate.open = true;
        }
        gate.cv.notify_all();

        struct timeval tv{};
        tv.tv_sec = 2;
        ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        std::vector<uint8_t> in;
        uint8_t buf[4096];
        for (;;) {
            ssize_t k = ::recv(fd, buf, sizeof(buf), 0);
            if (k > 0) { in.insert(in.end(), buf, buf + k); continue; }
            break;                              // FIN, or the read timed out
        }
        if (in.size() / kFrame == 1) ++agg.answered;

        const auto closedDeadline = std::chrono::steady_clock::now()
                                  + std::chrono::milliseconds(2000);
        while (std::chrono::steady_clock::now() < closedDeadline &&
               server.stats().closed.load() == 0)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        agg.bytesDropped += server.stats().bytesDropped.load();
        agg.queueDrops   += server.stats().queueDrops.load();

        ::close(fd);
        server.stop();
    }
    return agg;
}

void report(const char* label, bool peerFin, const Outcome& o) {
    std::printf("%s (peer %s)\n", label,
                peerFin ? "sends request + FIN" : "sends request, stays open");
    std::printf("  closeConnection() = %s, handler answered %ld, peer received %ld "
                "bytes / %ld frame(s)%s\n",
                o.closeAccepted ? "true" : "false", o.echoesQueued,
                o.answeredBytes, o.answeredFrames,
                o.truncated ? "  [read ended without a FIN]" : "");
    std::printf("  stats: framesRecv %llu framesSent %llu bytesDropped %llu "
                "queueDrops %llu closed %llu aborted %llu\n",
                o.framesRecv, o.framesSent, o.bytesDropped, o.queueDrops,
                o.closed, o.aborted);
}

}  // namespace

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);
    std::printf("closeConnection() racing a peer that sent its request and its FIN:\n"
                "is the request answered, or FIN'd over?\n\n");

    const Outcome a = run(true);
    report("A. close while request + FIN are unread", true, a);

    std::printf("\n");
    const Outcome b = run(false);
    report("B. control: identical, no FIN", false, b);

    std::printf("\n");
    const LateRun c = runLate(20);
    std::printf("C. the same close, but the FIN was already read before it was asked\n"
                "   %d iteration(s), closeConnection() accepted in %d, peer answered in %d, "
                "bytesDropped %llu queueDrops %llu\n",
                c.iterations, c.accepted, c.answered, c.bytesDropped, c.queueDrops);

    const long          expect = 1;
    const unsigned long long noDrop = 0;

    std::printf("\n");
    std::printf("  [%s] A: the handler ran and its answer reached the peer: %ld of %ld "
                "frame(s), %ld handoff(s)\n",
                (a.answeredFrames == expect && a.echoesQueued == expect) ? "PASS" : "FAIL",
                a.answeredFrames, expect, a.echoesQueued);
    std::printf("  [%s] A drops nothing it promised to answer: bytesDropped %llu, "
                "queueDrops %llu\n",
                (a.bytesDropped == noDrop && a.queueDrops == noDrop) ? "PASS" : "FAIL",
                a.bytesDropped, a.queueDrops);
    std::printf("  [%s] B (no FIN, same gate) answers and drops nothing: %ld frame(s), "
                "bytesDropped %llu\n",
                (b.answeredFrames == expect && b.bytesDropped == noDrop) ? "PASS" : "FAIL",
                b.answeredFrames, b.bytesDropped);
    std::printf("  [%s] C: the changed branch, taken with served == 0, still answers and "
                "still ends the connection: %d of %d accepted, %d of %d answered, "
                "bytesDropped %llu\n",
                (c.accepted > 0 && c.answered == c.iterations &&
                 c.bytesDropped == noDrop && c.queueDrops == noDrop) ? "PASS" : "FAIL",
                c.accepted, c.iterations, c.answered, c.iterations,
                c.bytesDropped);

    const bool ok = a.answeredFrames == expect && a.echoesQueued == expect &&
                    a.bytesDropped == noDrop && a.queueDrops == noDrop &&
                    b.answeredFrames == expect && b.bytesDropped == noDrop &&
                    c.accepted > 0 && c.answered == c.iterations &&
                    c.bytesDropped == noDrop && c.queueDrops == noDrop;
    return ok ? 0 : 1;
}
