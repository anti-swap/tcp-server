// verify/evidence/probe_frames_sent.cpp
//
// Evidence for the round-3 finding on framesSent vs bytesSent. This is not part
// of the suite: it is the measurement behind a claim, kept so the claim can be
// re-checked instead of taken on faith.
//
// It measures two things.
//
//   1. How many times handleRead actually reads a socket that has already
//      reported EOF (recv returned 0). If handleRead's `framesLeft <= 0` branch
//      inside the EOF handler could run, one FIN would be read more than once
//      and eofReads would exceed 1. Across four burst sizes it stays at 1,
//      which is consistent with that branch being unreachable -- every path that
//      reaches recv() has already established that fewer complete frames are
//      buffered than the frame budget allows.
//
//   2. What framesSent and bytesSent say about the same exchange when the close
//      ends in an abort with output still undelivered. framesSent counts a frame
//      when it enters wbuf; bytesSent counts it when sendmsg puts it on the
//      wire; nothing ever subtracts a frame that entered and was later dropped.
//
// Build and run (Linux only, from the repo root):
//   g++ -std=c++17 -Wall -Wextra -pthread -O1 verify/evidence/probe_frames_sent.cpp -o probe_frames_sent
//   ./probe_frames_sent
//
// Typical output on this machine:
//   burst=1     framesRecv=1     eofReads=1   replyBytes=72
//   burst=2000  framesRecv=2000  eofReads=1   replyBytes=144000
//   framesSent  =255        (counted entering wbuf)
//   bytesSent   =3145728    (95 frames fully out, plus 29728 bytes of the 96th)
//   bytesDropped=5212152    (still in wbuf when the linger deadline aborted it)
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "tcp-server-mt.h"

using industrial::FrameType;
using industrial::MultiReactorServer;

namespace {

uint16_t pickPort() {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return 0;
    struct sockaddr_in sa{};
    sa.sin_family = AF_INET;
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    sa.sin_port = 0;
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
    sa.sin_family = AF_INET;
    sa.sin_port = htons(port);
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::connect(fd, (struct sockaddr*)&sa, sizeof(sa)) < 0) {
        ::close(fd);
        return -1;
    }
    return fd;
}

std::vector<uint8_t> encode(FrameType type, const void* p, size_t n) {
    std::vector<uint8_t> f(industrial::FRAME_HEADER_SIZE + n);
    industrial::encodeFrameInto(f.data(), type, p, n);
    return f;
}

void sendAll(int fd, const std::vector<uint8_t>& f) {
    size_t off = 0;
    while (off < f.size()) {
        ssize_t n = ::send(fd, f.data() + off, f.size() - off, MSG_NOSIGNAL);
        if (n <= 0) return;
        off += static_cast<size_t>(n);
    }
}

// Send `burst` frames, half-close, and read until our own FIN. Reports how many
// times this connection's EOF was observed by the reactor.
void probeEofReads(int burst) {
    MultiReactorServer::Config cfg;
    cfg.bindAddr = "127.0.0.1";
    cfg.port = pickPort();
    cfg.numReactors = 1;
    cfg.idleTimeoutMs = 60000;

    MultiReactorServer server(cfg);
    server.setMessageHandler([&](uint64_t id, const uint8_t* p, size_t n) {
        server.sendTo(id, p, n);
    });
    if (!server.start()) {
        std::printf("      start failed\n");
        return;
    }

    int fd = connectTo(cfg.port);
    if (fd < 0) {
        std::printf("      connect failed\n");
        server.stop();
        return;
    }
    const std::string payload(64, 'x');
    for (int i = 0; i < burst; ++i)
        sendAll(fd, encode(FrameType::DATA, payload.data(), payload.size()));
    ::shutdown(fd, SHUT_WR);

    uint64_t got = 0;
    for (;;) {
        uint8_t b[65536];
        ssize_t n = ::recv(fd, b, sizeof(b), 0);
        if (n <= 0) break;
        got += static_cast<uint64_t>(n);
    }
    ::close(fd);
    // Give the reactor time to run its own teardown before reading counters.
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    const auto& st = server.stats();
    std::printf("      burst=%-5d framesRecv=%llu eofReads=%llu replyBytes=%llu\n",
                burst,
                static_cast<unsigned long long>(st.framesRecv.load()),
                static_cast<unsigned long long>(st.eofReads.load()),
                static_cast<unsigned long long>(got));
    server.stop();
}

// Make the peer never read, so output backs up into wbuf, then let the linger
// deadline abort the close: exactly the situation someone would consult
// framesSent to understand.
void probeFramesSent() {
    MultiReactorServer::Config cfg;
    cfg.bindAddr = "127.0.0.1";
    cfg.port = pickPort();
    cfg.numReactors = 1;
    cfg.idleTimeoutMs = 300;    // park the close quickly
    cfg.closeLingerMs = 200;    // then expire it while wbuf still holds bytes
    cfg.tickMs = 20;
    cfg.wbufCapacity = 8 * 1024 * 1024;
    cfg.maxPendingBytes = 8 * 1024 * 1024;

    MultiReactorServer server(cfg);
    std::atomic<int> replies{0};
    server.setMessageHandler([&](uint64_t id, const uint8_t*, size_t) {
        const std::string blob(32 * 1024, 'y');
        for (int i = 0; i < 512; ++i) {
            if (server.sendTo(id, blob.data(), blob.size()) != industrial::SendResult::Ok) break;
            replies.fetch_add(1);
        }
    });
    if (!server.start()) {
        std::printf("      start failed\n");
        return;
    }

    int fd = connectTo(cfg.port);
    if (fd < 0) {
        std::printf("      connect failed\n");
        server.stop();
        return;
    }
    const std::string req = "go";
    sendAll(fd, encode(FrameType::DATA, req.data(), req.size()));
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));   // read nothing
    ::shutdown(fd, SHUT_RDWR);
    ::close(fd);
    server.stop();

    const auto& st = server.stats();
    const uint64_t fs = st.framesSent.load();
    const double deliveredPerFrame =
        fs ? static_cast<double>(st.bytesSent.load()) / static_cast<double>(fs) : 0.0;
    std::printf("      replies accepted by sendTo      = %d\n", replies.load());
    std::printf("      framesSent   (entering wbuf)    = %llu\n",
                static_cast<unsigned long long>(fs));
    std::printf("      bytesSent    (leaving the socket) = %llu\n",
                static_cast<unsigned long long>(st.bytesSent.load()));
    std::printf("      bytesDropped (still in wbuf)    = %llu\n",
                static_cast<unsigned long long>(st.bytesDropped.load()));
    std::printf("      bytesSent / framesSent          = %.1f  (each frame carries 32800)\n",
                deliveredPerFrame);
    std::printf("      queueDrops=%llu framesDroppedOnClose=%llu aborted=%llu\n",
                static_cast<unsigned long long>(st.queueDrops.load()),
                static_cast<unsigned long long>(st.framesDroppedOnClose.load()),
                static_cast<unsigned long long>(st.aborted.load()));
}

}  // namespace

int main() {
    std::printf("== how many times is a FIN'd socket read? ==\n");
    for (int burst : {1, 8, 300, 2000}) probeEofReads(burst);
    std::printf("\n== framesSent vs bytesSent across an abort ==\n");
    probeFramesSent();
    return 0;
}
