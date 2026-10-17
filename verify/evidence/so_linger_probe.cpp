// Scratch probe (not part of the test suite): does this kernel turn
// SO_LINGER{on,0} into an RST that the peer can actually observe?
//
// The server was counted `aborted` internally yet the client saw an orderly FIN.
// Deciding whether that is a server defect or a platform limitation requires
// asking the kernel directly, with the server's own close sequence and nothing
// else in the way.
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <string>

static int listener() {
    int ln = ::socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in sa{};
    sa.sin_family = AF_INET;
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    sa.sin_port = 0;
    ::bind(ln, (struct sockaddr*)&sa, sizeof(sa));
    ::listen(ln, 4);
    return ln;
}

static unsigned portOf(int ln) {
    struct sockaddr_in sa{};
    socklen_t l = sizeof(sa);
    ::getsockname(ln, (struct sockaddr*)&sa, &l);
    return ntohs(sa.sin_port);
}

// What the peer ends up seeing after `sv` is closed the way the server closes an
// aborted connection.
static const char* observe(int ln, bool linger /* zero-linger on the server */,
                           bool clientSentFirst) {
    unsigned port = portOf(ln);
    int c = ::socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in sa{};
    sa.sin_family = AF_INET;
    sa.sin_port = htons(port);
    sa.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::connect(c, (struct sockaddr*)&sa, sizeof(sa)) < 0) return "connect failed";

    int sv = ::accept(ln, nullptr, nullptr);
    if (sv < 0) return "accept failed";

    if (clientSentFirst) {
        const char* junk = "0123456789abcdef";
        ::send(c, junk, 16, 0);
        char buf[64];
        ::usleep(50 * 1000);
        // Server reads everything so the kernel has no unread data queued.
        ssize_t n = ::recv(sv, buf, sizeof(buf), 0);
        (void)n;
    } else {
        ::send(sv, "hi", 2, 0);
        char b[8];
        ::recv(c, b, sizeof(b), 0);
    }

    if (linger) {
        struct linger lg{};
        lg.l_onoff = 1;
        lg.l_linger = 0;
        if (::setsockopt(sv, SOL_SOCKET, SO_LINGER, &lg, sizeof(lg)) < 0)
            std::printf("      setsockopt(SO_LINGER) failed: errno=%d (%s)\n",
                        errno, std::strerror(errno));
    }
    ::close(sv);

    // Give whatever is coming time to arrive, then ask once.
    struct timeval tv{};
    tv.tv_sec = 1;
    ::setsockopt(c, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    char b[256];
    ssize_t n = ::recv(c, b, sizeof(b), 0);
    if (n > 0) return "data (!?)";
    if (n == 0) return "FIN (orderly)";
    if (errno == EAGAIN || errno == EWOULDBLOCK) return "timeout, nothing arrived";
    if (errno == ECONNRESET) return "RST";
    static std::string why;
    why = std::string("errno=") + std::to_string(errno) + " (" + std::strerror(errno) + ")";
    return why.c_str();
}

int main() {
    int ln = listener();

    std::printf("SO_LINGER{on,0} -> %s\n", observe(ln, true, false));
    std::printf("plain close     -> %s\n", observe(ln, false, false));
    std::printf("SO_LINGER{on,0} after the client's bytes were consumed -> %s\n",
                observe(ln, true, true));
    std::printf("plain close     after the client's bytes were consumed -> %s\n",
                observe(ln, false, true));

    ::close(ln);
    return 0;
}
