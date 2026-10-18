// verify/evidence/probe_epipe.cpp
//
// Evidence for round-3 open item #3, and a correction to it.
//
// The report called the EPIPE branch of drainWbufToSocket "very likely
// unreachable": every route to it was thought to be intercepted elsewhere --
// our own shutdown(SHUT_WR) by the finSent guards in handleWrite and parkClose,
// a peer reset by the EPOLLERR/EPOLLHUP branch of loop(). It recommended adding
// a defensive disarmWrite() and leaving it at that.
//
// Reachable or not was never measured. This does, and the branch turns out to
// be reachable from the outside with no unusual cooperation from anyone: a peer
// that stops reading and disappears gives the server plenty of queued bytes and
// then a socket whose writes fail permanently.
//
// What that meant in practice is the second finding. drainWbufToSocket returned
// the same 0 for two unrelated outcomes -- "wbuf is now empty, every queued byte
// reached the kernel" and "EPIPE, nothing leaving again" -- so no caller could
// tell them apart, and each one inferred it from `wbuf.empty()` at its own call
// site without saying so. Fixing callers without splitting those outcomes turns
// every healthy drain into a teardown: the first attempt at it took the whole
// close_semantics suite red.
//
// This file is not part of the suite. It is the measurement:
//
//   With the peer holding data unread and vanishing:
//     construction A (abrupt RST, reply queued):  epipeWrites=N
//     construction B (abrupt RST, no reply queued)
//     construction C (orderly close, reply queued)
//
// Build and run (Linux only, from the repo root):
//   g++ -std=c++17 -Wall -Wextra -pthread -O1 verify/evidence/probe_epipe.cpp -o probe_epipe
//   ./probe_epipe
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
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

bool waitFor(int ms, const std::function<bool()>& pred) {
    for (int i = 0; i < ms / 10; ++i) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return pred();
}

// One run: `replyFrames` frames of 32KB are queued to the peer, then the peer
// leaves. `abrupt` decides whether its socket goes away with an RST (SO_LINGER
// {on,0} or unread data outstanding) or with an orderly FIN.
void probe(const char* label, int replyFrames, int exitStyle) {
    MultiReactorServer::Config cfg;
    cfg.bindAddr      = "127.0.0.1";
    cfg.port          = pickPort();
    cfg.numReactors   = 1;
    cfg.idleTimeoutMs = 60000;      // nothing here should be ended by a timer
    cfg.wbufCapacity  = 32 * 1024 * 1024;
    cfg.maxPendingBytes = 32 * 1024 * 1024;

    MultiReactorServer server(cfg);
    server.setMessageHandler([&](uint64_t id, const uint8_t* p, size_t n) {
        if (replyFrames <= 0) return;
        // One request, many frames of answer: far more than the loopback socket
        // buffers will absorb, so plenty is still queued when the peer leaves.
        std::vector<uint8_t> payload(32 * 1024, 'x');
        for (int i = 0; i < replyFrames; ++i)
            server.sendTo(id, payload.data(), payload.size());
        (void)p;
        (void)n;
    });
    if (!server.start()) {
        std::printf("%-46s start failed\n", label);
        return;
    }

    int fd = connectTo(cfg.port);
    if (fd < 0) {
        std::printf("%-46s connect failed\n", label);
        server.stop();
        return;
    }
    auto req = encode(FrameType::DATA, "please answer", 13);
    sendAll(fd, req);
    // Let the handler queue its answer and the drain stall against the full
    // socket buffer before the peer disappears.
    std::this_thread::sleep_for(std::chrono::milliseconds(150));

    // Four ways for the peer to disappear while bytes are still owed to it.
    // exitStyle 0 = SO_LINGER{on,0} then close (asks for an RST; this kernel
    // answers with a FIN instead -- see verify/evidence/so_linger_probe.cpp,
    // which is why that alone never decides anything here), 1 = plain close(),
    // 2 = close() with unread data outstanding (the textbook RST trigger),
    // 3 = orderly shutdown(SHUT_WR) and nothing else.
    if (exitStyle == 0) {
        struct linger lg{};
        lg.l_onoff  = 1;
        lg.l_linger = 0;
        ::setsockopt(fd, SOL_SOCKET, SO_LINGER, &lg, sizeof(lg));
        ::close(fd);
    } else if (exitStyle == 1 || exitStyle == 2) {
        ::close(fd);
    } else {
        ::shutdown(fd, SHUT_WR);
        ::close(fd);
    }

    waitFor(3000, [&] { return server.connectionCount() == 0; });

    const auto& st = server.stats();
    std::printf("%-46s epipeWrites=%llu eofReads=%llu bytesSent=%llu "
                "bytesDropped=%llu aborted=%llu closed=%llu stillOpen=%zu\n",
                label,
                (unsigned long long)st.epipeWrites.load(),
                (unsigned long long)st.eofReads.load(),
                (unsigned long long)st.bytesSent.load(),
                (unsigned long long)st.bytesDropped.load(),
                (unsigned long long)st.aborted.load(),
                (unsigned long long)st.closed.load(),
                server.connectionCount());
    server.stop();
}

}  // namespace

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);
    std::printf("Does anything reach drainWbufToSocket's EPIPE branch?\n");
    probe("A SO_LINGER{on,0} close, 8MB queued", 256, 0);
    probe("B SO_LINGER{on,0} close, nothing queued", 0, 0);
    probe("C plain close(), 8MB queued", 256, 1);
    probe("D plain close() with data unread", 256, 2);
    probe("E orderly SHUT_WR then close", 256, 3);
    return 0;
}
