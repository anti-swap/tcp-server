// verify/ping_pong.cpp -- the read path's most conditional branch, executed.
//
// parseFrames() answers PING itself: a reservation CAS loop against
// maxPendingBytes, a refusal that tears the connection down, and a "we are
// already closing" case that drops the reply and counts it apart. Nothing under
// verify/ ever ran any of it -- grep found no PING and no PONG in the whole
// suite -- so the branch was correct and unguarded, which is a worse state than
// being wrong: it could be broken by any edit and every configuration would
// still come back green.
//
// What is asserted, and why each one is the point rather than a detail:
//
//   1. A PING on a healthy connection gets exactly one PONG, and the PONG is a
//      frame the far side's own parser accepts -- type PONG, payload empty.
//   2. The PONG is counted like any other frame. framesSent and bytesSent are
//      reconcilable only if every frame that leaves is counted, and a PONG is a
//      frame that leaves: bytesSent must be 8 * PONG, 8 being the header.
//   3. A PING that lands after the close was asked for is dropped and counted
//      as framesDroppedOnClose. Answering it would put a frame out past the FIN,
//      which is the thing the whole close sequence exists to prevent.
//
// Build (Linux only):
//   g++ -std=c++17 -Wall -Wextra -pthread verify/ping_pong.cpp -o ping_pong
//
// Exit code 0 = the PING branch answers, counts, and stays behind the FIN.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <thread>
#include <vector>

#include "tcp-server-mt.h"

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

std::vector<uint8_t> frame(FrameType t, const void* p, size_t n) {
    std::vector<uint8_t> f(industrial::FRAME_HEADER_SIZE + n);
    industrial::encodeFrameInto(f.data(), t, p, n);
    return f;
}

// Blocks up to ms for one complete frame, parsed with the same header layout
// the server uses -- a PONG the peer cannot parse is not an answer.
bool readFrame(int fd, int ms, uint16_t& type, uint32_t& len) {
    std::vector<uint8_t> buf;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(ms);
    for (;;) {
        if (buf.size() >= industrial::FRAME_HEADER_SIZE) {
            const uint32_t l = (static_cast<uint32_t>(buf[4]) << 24)
                             | (static_cast<uint32_t>(buf[5]) << 16)
                             | (static_cast<uint32_t>(buf[6]) <<  8)
                             |  static_cast<uint32_t>(buf[7]);
            if (buf.size() >= industrial::FRAME_HEADER_SIZE + l) {
                type = static_cast<uint16_t>((static_cast<uint16_t>(buf[2]) << 8) | buf[3]);
                len  = l;
                return true;
            }
        }
        if (std::chrono::steady_clock::now() >= deadline) return false;
        uint8_t tmp[512];
        const ssize_t r = ::recv(fd, tmp, sizeof(tmp), MSG_DONTWAIT);
        if (r > 0) buf.insert(buf.end(), tmp, tmp + r);
        else if (r == 0) return false;
        else std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
}

MultiReactorServer::Config pingConfig() {
    MultiReactorServer::Config cfg;
    cfg.bindAddr      = "127.0.0.1";
    cfg.port          = pickPort();
    cfg.numReactors   = 1;
    cfg.tickMs        = 50;
    cfg.idleTimeoutMs = 60000;   // nothing here may end by a timer
    cfg.closeLingerMs = 3000;
    return cfg;
}

void caseHealthy() {
    std::printf("healthy: five PINGs on a connection that is not going anywhere\n");
    MultiReactorServer::Config cfg = pingConfig();
    MultiReactorServer server(cfg);
    server.setMessageHandler([](uint64_t, const uint8_t*, size_t) {});
    if (!server.start()) { expect(false, "start() ok"); return; }

    int fd = connectTo(cfg.port);
    if (fd < 0) { expect(false, "client connected"); server.stop(); return; }

    const int kPings = 5;
    int got = 0, malformed = 0;
    for (int i = 0; i < kPings; ++i) {
        const std::vector<uint8_t> f = frame(FrameType::PING, nullptr, 0);
        if (!sendAll(fd, f.data(), f.size())) break;
        uint16_t t = 0; uint32_t l = 0;
        if (!readFrame(fd, 500, t, l)) continue;
        if (t == static_cast<uint16_t>(FrameType::PONG) && l == 0) ++got;
        else ++malformed;
    }

    const unsigned long long framesSent = server.stats().framesSent.load();
    const unsigned long long bytesSent  = server.stats().bytesSent.load();
    std::printf("      PONG %d of %d (malformed %d), framesSent %llu bytesSent %llu\n",
                got, kPings, malformed, framesSent, bytesSent);

    expect(got == kPings, "every PING got a well-formed PONG");
    expect(framesSent == static_cast<unsigned long long>(kPings),
           "the PONGs are counted as frames");
    expect(bytesSent == static_cast<unsigned long long>(kPings) *
                            industrial::FRAME_HEADER_SIZE,
           "bytesSent is exactly header-size per PONG, so the counters reconcile");

    ::close(fd);
    server.stop();
}

// The PING is sent in flight with the close: the close has been asked for, has
// not finished, and the peer is still talking. That is the case the branch's
// third arm exists for.
void caseWhileClosing() {
    std::printf("while closing: five PINGs sent as the close goes out\n");
    MultiReactorServer::Config cfg = pingConfig();
    MultiReactorServer server(cfg);
    std::atomic<uint64_t> id{0};
    server.setMessageHandler([&](uint64_t cid, const uint8_t*, size_t) { id.store(cid); });
    if (!server.start()) { expect(false, "start() ok"); return; }

    int fd = connectTo(cfg.port);
    if (fd < 0) { expect(false, "client connected"); server.stop(); return; }

    const std::vector<uint8_t> hello = frame(FrameType::DATA, "hi", 2);
    (void)sendAll(fd, hello.data(), hello.size());
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    // closeConnection() only asks: the close reaches the connection through the
    // reactor's queue, so a PING already in flight may legitimately be answered
    // before it takes effect. Asserting on what happens from the moment of the
    // request would be asserting on that race, and it does go both ways (5/0,
    // 3/2 and 4/1 across three runs).
    //
    // So: PING until one is counted as dropped, which is the observable point at
    // which the close is in effect, and assert about everything after that.
    server.closeConnection(id.load());
    unsigned long long dropped = 0;
    for (int i = 0; i < 20 && dropped == 0; ++i) {
        const std::vector<uint8_t> f = frame(FrameType::PING, nullptr, 0);
        if (!sendAll(fd, f.data(), f.size())) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
        dropped = server.stats().framesDroppedOnClose.load();
    }
    const unsigned long long sentAfterCloseBegan = server.stats().framesSent.load();

    for (int i = 0; i < 5; ++i) {
        const std::vector<uint8_t> f = frame(FrameType::PING, nullptr, 0);
        if (!sendAll(fd, f.data(), f.size())) break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    const unsigned long long droppedNow = server.stats().framesDroppedOnClose.load();
    const unsigned long long sentNow    = server.stats().framesSent.load();
    const unsigned long long aborted    = server.stats().aborted.load();
    std::printf("      once closing: PINGs dropped +%llu, framesSent %llu -> %llu, "
                "aborted %llu\n",
                droppedNow - dropped, sentAfterCloseBegan, sentNow, aborted);

    expect(dropped >= 1, "a PING reaches a connection that is closing");
    expect(droppedNow > dropped, "PINGs arriving while closing are counted as dropped");
    expect(sentNow == sentAfterCloseBegan,
           "and none of them is answered: no frame leaves after the close began");
    expect(aborted == 0, "the close stayed graceful");

    ::close(fd);
    server.stop();
}

}  // namespace

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);
    caseHealthy();
    caseWhileClosing();
    std::printf("\n%s (%d failure%s)\n", gFailures == 0 ? "ALL PASS" : "FAILURES",
                gFailures, gFailures == 1 ? "" : "s");
    return gFailures == 0 ? 0 : 1;
}
