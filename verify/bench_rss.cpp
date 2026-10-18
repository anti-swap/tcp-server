// verify/bench_rss.cpp -- resident memory per idle connection.
//
// The question a server's Config answers for you: "maxConnections = 10000" is
// a promise about descriptors, and this says what else it costs. Buffers are
// sized when a connection is accepted, so the number is flat per connection and
// independent of traffic -- which is exactly why it is worth measuring once and
// then keeping true.
//
//   bench_rss <connections>
//
// Prints one line: connections, RSS before and after, and the per-connection
// delta. The baseline is taken after start() and before any client connects, so
// the fixed cost of the server itself is not attributed to connections.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <vector>

#include "tcp-server-mt.h"

using industrial::MultiReactorServer;

namespace {

long rssKb() {
    FILE* f = std::fopen("/proc/self/status", "r");
    if (!f) return -1;
    char line[256];
    long kb = -1;
    while (std::fgets(line, sizeof(line), f)) {
        if (std::sscanf(line, "VmRSS: %ld kB", &kb) == 1) break;
    }
    std::fclose(f);
    return kb;
}

int connectTo(uint16_t port) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
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

}  // namespace

int main(int argc, char** argv) {
    const int n = argc > 1 ? std::atoi(argv[1]) : 1000;

    MultiReactorServer::Config cfg;
    cfg.bindAddr      = "127.0.0.1";
    cfg.port          = 0;
    cfg.numReactors   = 1;
    cfg.tickMs        = 50;
    cfg.idleTimeoutMs = 600000;   // nothing here may be reclaimed mid-measurement

    MultiReactorServer server(cfg);
    server.setMessageHandler([](uint64_t, const uint8_t*, size_t) {});
    if (!server.start()) return 2;

    const uint16_t port = server.port();
    const long before   = rssKb();

    std::vector<int> fds;
    fds.reserve(static_cast<size_t>(n));
    for (int i = 0; i < n; ++i) {
        const int fd = connectTo(port);
        if (fd < 0) break;
        fds.push_back(fd);
    }
    // The accept loop hands connections to reactors asynchronously; the buffers
    // are allocated on the reactor side, so wait for the registry to catch up
    // before reading RSS. A fixed sleep would be either flaky or slow.
    for (int i = 0; i < 200 && server.connectionCount() < fds.size(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    const long after = rssKb();
    std::printf("rss connections=%zu delta_kb=%ld per_conn_kb=%.1f\n",
                server.connectionCount(), after - before,
                fds.empty() ? 0.0
                            : static_cast<double>(after - before) / fds.size());

    server.stop();
    for (int fd : fds) ::close(fd);
    return 0;
}
