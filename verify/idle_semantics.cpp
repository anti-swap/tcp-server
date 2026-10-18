// verify/idle_semantics.cpp -- what "idle" is measured in.
//
// An idle timeout is a promise about silence: if the peer is still sending, the
// deadline must not fire. This case pins down what counts as silence, because
// the answer is not obvious from the outside and the two candidates differ by
// exactly one situation -- a peer inside a frame it has not finished sending.
//
//   - bytes:      the deadline means "no byte arrived". A peer dribbling one
//                 large frame over a slow link is active and must be kept.
//   - frames:     the deadline means "no complete frame arrived". That same peer
//                 is idle for as long as the frame takes to arrive, and is closed
//                 mid-transfer while bytesRecv is still climbing.
//
// The second one is what the file used to do: lastRecvMs was refreshed only on
// the line in parseFrames() that consumes a complete frame, so bytes that had
// arrived but had not completed a frame refreshed nothing. A 64KB frame on a
// link slower than idleTimeoutMs was closed in the middle of itself.
//
// Three peers, all at the same byte rate, on a 600ms deadline:
//
//   A. half-frame  -- one frame declared, dribbled in pieces, never completed.
//   B. silent      -- connected and then nothing. Must be closed: this is the
//                     case the deadline exists for, and it is the guard against
//                     a fix that simply stops the timeout from firing at all.
//   C. full-frames -- the same pieces, each its own complete frame.
//
// A and C must survive; B must not.
//
// Build (Linux only):
//   g++ -std=c++17 -Wall -Wextra -pthread verify/idle_semantics.cpp -o idle_semantics
//
// Exit code 0 = the deadline means no bytes arrived.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <functional>
#include <string>
#include <thread>
#include <vector>

#include "../tcp-server-mt.h"

using industrial::FrameType;
using industrial::MultiReactorServer;

namespace {

int gFailures = 0;

void expect(bool ok, const char* what) {
    std::printf("  [%s] %s\n", ok ? "PASS" : "FAIL", what);
    if (!ok) ++gFailures;
}

uint16_t pickPort() {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return 0;
    struct sockaddr_in sa{};
    sa.sin_family      = AF_INET;
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    sa.sin_port        = 0;
    if (::bind(fd, (struct sockaddr*)&sa, sizeof(sa)) < 0) {
        ::close(fd);
        return 0;
    }
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
    if (::connect(fd, (struct sockaddr*)&sa, sizeof(sa)) < 0) {
        ::close(fd);
        return -1;
    }
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

// 600ms idle, checked every 50ms: a transfer that keeps sending crosses the
// deadline twice over the ~1.6s each case runs, so "the deadline did not fire"
// cannot be explained by the tick simply not having come around yet.
MultiReactorServer::Config idleConfig() {
    MultiReactorServer::Config cfg;
    cfg.bindAddr      = "127.0.0.1";
    cfg.port          = pickPort();
    cfg.numReactors   = 1;
    cfg.tickMs        = 50;
    cfg.idleTimeoutMs = 600;
    cfg.closeLingerMs = 1000;
    return cfg;
}

// True once the server's FIN has reached the peer.
bool hungUp(int fd) {
    uint8_t probe[64];
    const ssize_t r = ::recv(fd, probe, sizeof(probe), MSG_DONTWAIT);
    return r == 0;
}

// One frame declared as kTotal bytes, delivered kChunk bytes every 150ms and
// never finished: everything a slow link does to a large frame, with the peer
// never once going quiet.
void caseHalfFrame() {
    std::printf("A. half-frame: one frame declared, dribbled, never completed\n");
    MultiReactorServer::Config cfg = idleConfig();
    MultiReactorServer server(cfg);
    server.setMessageHandler([](uint64_t, const uint8_t*, size_t) {});
    if (!server.start()) { expect(false, "start() ok"); return; }

    int fd = connectTo(cfg.port);
    if (fd < 0) { expect(false, "client connected"); server.stop(); return; }

    const size_t kChunk = 2000;
    const size_t kTotal = 14000;
    uint8_t hdr[industrial::FRAME_HEADER_SIZE];
    industrial::writeFrameHeader(hdr, FrameType::DATA, kTotal);
    if (!sendAll(fd, hdr, sizeof(hdr))) { expect(false, "header sent"); ::close(fd); server.stop(); return; }

    bool   fin   = false;
    size_t sent  = 0;
    for (int i = 0; i < static_cast<int>(kTotal / kChunk); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(150));
        std::vector<uint8_t> body(kChunk, 'x');
        if (!sendAll(fd, body.data(), body.size())) break;
        sent += kChunk;
        if (hungUp(fd)) { fin = true; break; }
    }

    const unsigned long long recv = server.stats().bytesRecv.load();
    const unsigned long long idle = server.stats().idleTimeouts.load();
    std::printf("      sent %zu B, server received %llu B, idleTimeouts %llu\n",
                sent, recv, idle);

    expect(recv > 0, "the bytes arrived (the peer was not silent)");
    expect(!fin, "a peer that never stopped sending is not closed for idling");
    expect(idle == 0, "the idle deadline did not fire on a peer inside a frame");

    ::close(fd);
    server.stop();
}

// The control: a peer that really is silent still has to be reclaimed, or the
// fix for the case above is just the timeout being switched off.
void caseSilent() {
    std::printf("B. silent: connected, then nothing at all\n");
    MultiReactorServer::Config cfg = idleConfig();
    MultiReactorServer server(cfg);
    server.setMessageHandler([](uint64_t, const uint8_t*, size_t) {});
    if (!server.start()) { expect(false, "start() ok"); return; }

    int fd = connectTo(cfg.port);
    if (fd < 0) { expect(false, "client connected"); server.stop(); return; }

    bool fin = false;
    for (int i = 0; i < 150 && !fin; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        fin = hungUp(fd);
    }

    const unsigned long long idle = server.stats().idleTimeouts.load();
    std::printf("      idleTimeouts %llu, server FIN'd: %s\n", idle, fin ? "yes" : "no");
    expect(idle >= 1, "a silent peer is still reclaimed");
    expect(fin, "and the peer sees it");

    ::close(fd);
    server.stop();
}

// Same bytes, same rate, each piece a complete frame: what the clock used to
// require, kept here so narrowing it to bytes cannot be mistaken for the clock
// having stopped working.
void caseFullFrames() {
    std::printf("C. full-frames: the same pieces, each a complete frame\n");
    MultiReactorServer::Config cfg = idleConfig();
    MultiReactorServer server(cfg);
    std::atomic<long> frames{0};
    server.setMessageHandler([&](uint64_t, const uint8_t*, size_t) { frames.fetch_add(1); });
    if (!server.start()) { expect(false, "start() ok"); return; }

    int fd = connectTo(cfg.port);
    if (fd < 0) { expect(false, "client connected"); server.stop(); return; }

    const size_t kChunk = 2000;
    bool   fin = false;
    size_t sent = 0;
    for (int i = 0; i < 7; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(150));
        std::vector<uint8_t> f(industrial::FRAME_HEADER_SIZE + kChunk);
        industrial::encodeFrameInto(f.data(), FrameType::DATA, nullptr, kChunk);
        std::memset(f.data() + industrial::FRAME_HEADER_SIZE, 'x', kChunk);
        if (!sendAll(fd, f.data(), f.size())) break;
        sent += f.size();
        if (hungUp(fd)) { fin = true; break; }
    }

    const unsigned long long idle = server.stats().idleTimeouts.load();
    std::printf("      sent %zu B in %ld frames, idleTimeouts %llu\n",
                sent, frames.load(), idle);
    expect(!fin, "a peer sending complete frames is not closed for idling");
    expect(frames.load() >= 5, "the frames were delivered and dispatched");
    expect(idle == 0, "the idle deadline did not fire");

    ::close(fd);
    server.stop();
}

// The consequence of A, and the question its fix leaves open: if a peer inside
// a frame is no longer idle-reclaimed, what reclaims it?
//
// Answer, measured rather than assumed: the same clock, the moment the peer
// actually stops. A frame it never finishes cannot grow without bound -- the
// header's length is capped at MAX_FRAME_PAYLOAD (a larger one is closed as
// "frame too large" on arrival) and rbuf is required to hold the largest legal
// frame, so "rbuf full with an incomplete frame" is unreachable under start()'s
// validation. A peer can stall inside a frame for as long as it keeps sending;
// the instant it stops, it is a silent peer again.
//
// So: send a legal maximum frame all but one byte of it, then stop talking.
void caseHalfFrameThenSilent() {
    std::printf("D. half-frame that stops mid-frame: is it reclaimed then?\n");
    MultiReactorServer::Config cfg = idleConfig();
    MultiReactorServer server(cfg);
    server.setMessageHandler([](uint64_t, const uint8_t*, size_t) {});
    if (!server.start()) { expect(false, "start() ok"); return; }

    int fd = connectTo(cfg.port);
    if (fd < 0) { expect(false, "client connected"); server.stop(); return; }

    const size_t kChunk = 2000;
    const size_t kTotal = industrial::MAX_FRAME_PAYLOAD;
    uint8_t hdr[industrial::FRAME_HEADER_SIZE];
    industrial::writeFrameHeader(hdr, FrameType::DATA, kTotal);
    if (!sendAll(fd, hdr, sizeof(hdr))) { expect(false, "header sent"); ::close(fd); server.stop(); return; }

    // All but the last byte, delivered fast enough that the clock never expires
    // while the peer is still active.
    size_t sent = 0;
    while (sent + kChunk < kTotal) {
        std::vector<uint8_t> body(kChunk, 'x');
        if (!sendAll(fd, body.data(), body.size())) break;
        sent += kChunk;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    const unsigned long long idleWhileSending = server.stats().idleTimeouts.load();

    // Now silent, inside a frame, for well past the deadline.
    bool fin = false;
    for (int i = 0; i < 150 && !fin; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        fin = hungUp(fd);
    }

    const unsigned long long idle = server.stats().idleTimeouts.load();
    std::printf("      sent %zu of %zu bytes, then stopped; idleTimeouts %llu "
                "(was %llu while sending), FIN'd: %s\n",
                sent, kTotal, idle, idleWhileSending, fin ? "yes" : "no");

    expect(idleWhileSending == 0, "still not reclaimed while it was sending");
    expect(fin, "reclaimed once it went silent, even mid-frame");
    expect(idle >= 1, "by the idle clock, which is the only thing that changed");

    ::close(fd);
    server.stop();
}

}  // namespace

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);
    std::printf("idleTimeoutMs=600, tickMs=50; every case runs ~1.6s at the same\n"
                "byte rate. Only whether the bytes form frames differs.\n\n");
    caseHalfFrame();
    caseSilent();
    caseFullFrames();
    caseHalfFrameThenSilent();
    std::printf("\n%s (%d failure%s)\n", gFailures == 0 ? "ALL PASS" : "FAILURES",
                gFailures, gFailures == 1 ? "" : "s");
    return gFailures == 0 ? 0 : 1;
}
