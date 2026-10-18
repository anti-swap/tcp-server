// verify/evidence/probe_api_contract.cpp
//
// Two things nobody has asked this header:
//
//   1. Config::port defaults to 0, which tells the kernel to pick the port, and
//      start() accepts it. Until port() existed, nothing reported which port was
//      picked: a server built from a default Config started, reported success,
//      and could not be reached by anything that did not guess. Case A below is
//      now the assertion that closes that -- it connects to port() rather than
//      to a number the probe picked.
//
//   2. sendTo() has five outcomes and no case under verify/ asserts any of the
//      four that are not Ok. They are the whole of what an application can
//      learn from a send, so "correct but untested" here means "correct but
//      free to change".
//
// Build and run (Linux only, from the repo root):
//   g++ -std=c++17 -Wall -Wextra -pthread -O1 probe_api_contract.cpp
//   ./probe_api_contract

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "tcp-server-mt.h"

using industrial::MultiReactorServer;
using industrial::SendResult;

namespace {

// What "the header answers the question" means for this file: a printed line
// nobody has to read is not a check. Everything this probe claims about the API
// goes through here, so the exit code says whether the claim still holds.
int g_failures = 0;

void check(bool ok, const char* what) {
    std::printf("   [%s] %s\n", ok ? "PASS" : "FAIL", what);
    if (!ok) ++g_failures;
}

const char* name(SendResult r) {
    switch (r) {
        case SendResult::Ok:               return "Ok";
        case SendResult::NotConnected:     return "NotConnected";
        case SendResult::Closed:           return "Closed";
        case SendResult::TooLarge:         return "TooLarge";
        case SendResult::Overloaded:       return "Overloaded";
        case SendResult::InvalidArgument:  return "InvalidArgument";
    }
    return "?";
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

// A. A default Config's port is 0, which asks the kernel to choose. port() has
//    to answer with what was chosen -- otherwise start() returning true is the
//    only news an application ever gets about a server nobody can reach.
void caseDefaultPort() {
    std::printf("A. Config::port = 0 (the default): start() takes it, and port()\n"
                "   says where it landed\n");

    MultiReactorServer::Config cfg;      // port is 0 by default
    cfg.numReactors = 1;
    MultiReactorServer server(cfg);
    check(server.port() == 0, "port() is 0 before start(): nothing is bound");
    const bool started = server.start();
    check(started, "start() succeeds with the default Config");

    // cfg.port is not written back -- a second start() after a stop() must ask
    // for a fresh port, not re-bind last run's throwaway number.
    check(cfg.port == 0, "cfg.port is left alone: no write-back");
    const uint16_t bound = server.port();
    check(bound != 0, "port() reports the port the kernel chose");
    std::printf("   kernel chose %u\n", (unsigned)bound);

    int fd = connectTo(bound);
    check(fd >= 0, "a client can connect to port() without being told the number");
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    check(server.connectionCount() == 1, "and the server counts that connection");
    if (fd >= 0) ::close(fd);

    server.stop();
    check(server.port() == 0, "port() is 0 again after stop(): no socket, no port");

    // The same question on the other address family, because it is a different
    // branch: a v6 listener resolves its port through sockaddr_in6, and a
    // port() that only knew sockaddr_in would answer 0 here.
    MultiReactorServer::Config cfg6;
    cfg6.bindAddr    = "::";
    cfg6.numReactors = 1;
    MultiReactorServer s6(cfg6);
    if (!s6.start()) {
        std::printf("   [SKIP] no IPv6 listener available on this host\n");
    } else {
        check(s6.port() != 0, "port() resolves an IPv6 listener too");
        s6.stop();
    }

    // For contrast: an explicit port is reported unchanged, so port() is not
    // just "a number that appeared" -- it is the socket's own answer.
    MultiReactorServer::Config cfg2;
    cfg2.port        = pickPort();
    cfg2.numReactors = 1;
    MultiReactorServer s2(cfg2);
    if (!s2.start()) {
        check(false, "start() with an explicit port");
    } else {
        check(s2.port() == cfg2.port, "port() echoes an explicit Config::port");
        s2.stop();
    }
    std::printf("\n");
}

// B. The four outcomes that are not Ok.
void caseSendResults() {
    std::printf("B. sendTo()'s four failure outcomes, none of which any case asserts\n");

    MultiReactorServer::Config cfg;
    cfg.port            = pickPort();
    cfg.numReactors     = 1;
    cfg.maxPendingBytes = industrial::FRAME_HEADER_SIZE * 4;   // tiny on purpose
    cfg.wbufCapacity    = industrial::FRAME_HEADER_SIZE * 4;
    MultiReactorServer server(cfg);
    server.setMessageHandler([](uint64_t, const uint8_t*, size_t) {});
    if (!server.start()) { std::printf("   start failed\n\n"); return; }

    int fd = connectTo(cfg.port);
    if (fd < 0) { std::printf("   connect failed\n\n"); server.stop(); return; }
    std::this_thread::sleep_for(std::chrono::milliseconds(120));

    const std::vector<uint64_t> ids = server.connectionIds();
    if (ids.empty()) { std::printf("   no connection\n\n"); ::close(fd); server.stop(); return; }
    const uint64_t id = ids[0];

    std::printf("   to a live connection, 4 bytes : %s\n",
                name(server.sendTo(id, "abcd", 4)));
    std::printf("   to an id that was never issued: %s\n",
                name(server.sendTo(999999, "abcd", 4)));
    std::printf("   payload over MAX_FRAME_PAYLOAD: %s\n",
                name(server.sendTo(id, "abcd", industrial::MAX_FRAME_PAYLOAD + 1)));
    std::printf("   data = nullptr with len = 5   : %s\n",
                name(server.sendTo(id, nullptr, 5)));

    // Drive pendingBytes past its ceiling with a peer that never reads.
    for (int i = 0; i < 200; ++i) server.sendTo(id, "0123456789", 10);
    std::printf("   after saturating maxPendingBytes: %s\n",
                name(server.sendTo(id, "abcd", 4)));

    // Now close it and send again. The question is not only what sendTo answers
    // -- it is whether what it answered Ok to reached the socket at all.
    server.closeConnection(id);
    // Long enough for the close to be in effect and for the backlog above to
    // have been refused: those drops belong to the close, not to the send under
    // test, and only a baseline taken after they land can say which is which.
    std::this_thread::sleep_for(std::chrono::milliseconds(400));

    const unsigned long long qd0 = server.stats().queueDrops.load();
    const unsigned long long bd0 = server.stats().bytesDropped.load();
    const unsigned long long bs0 = server.stats().bytesSent.load();
    const unsigned long long cl0 = server.stats().closed.load();

    check(server.connectionCount() == 1, "the closing connection is still in the table");
    const SendResult after = server.sendTo(id, "abcd", 4);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    check(after == SendResult::Closed,
          "sendTo() to a connection whose close is in progress: Closed, not Ok");
    check(server.stats().queueDrops.load() - qd0 == 0 &&
          server.stats().bytesDropped.load() - bd0 == 0,
          "and what it answered bought no drops either");
    std::printf("     bytesSent +%llu, queueDrops +%llu, "
                "bytesDropped +%llu, closed +%llu\n",
                server.stats().bytesSent.load() - bs0,
                server.stats().queueDrops.load() - qd0,
                server.stats().bytesDropped.load() - bd0,
                server.stats().closed.load() - cl0);

    ::close(fd);
    server.stop();
    std::printf("\n");
}

}  // namespace

int main() {
    setvbuf(stdout, nullptr, _IONBF, 0);
    caseDefaultPort();
    caseSendResults();
    std::printf("%s (%d check%s failed)\n",
                g_failures ? "FAILED" : "PASSED", g_failures,
                g_failures == 1 ? "" : "s");
    return g_failures ? 1 : 0;
}
