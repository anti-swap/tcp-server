// Minimal POSIX-family declarations so the Linux-only header can be
// type-checked with a non-Linux toolchain. This is a SYNTAX/SEMANTIC shim, not
// an implementation: it exists so g++ can resolve epoll/eventfd/socket symbols.
// Nothing here is linked; the real build must use a Linux toolchain.
#pragma once

#include <cstdint>
#include <cstddef>
#include <sys/types.h>

// mingw's sys/types.h does not define the POSIX socket types.
typedef unsigned int socklen_t;
typedef unsigned int u_int32_t;

// ---------- errno ----------
#define EINTR 4
#define EAGAIN 11
#define EWOULDBLOCK EAGAIN
#define ENOENT 2

// ---------- fcntl / poll ----------
#define EPOLLIN 0x001
#define EPOLLOUT 0x004
#define EPOLLERR 0x008
#define EPOLLHUP 0x010
#define EPOLL_CLOEXEC 0x80000
#define EPOLL_CTL_ADD 1
#define EPOLL_CTL_DEL 2
#define EPOLL_CTL_MOD 3
#define EFD_NONBLOCK 0x800
#define EFD_CLOEXEC 0x80000

typedef union epoll_data {
    void* ptr;
    int fd;
    uint32_t u32;
    uint64_t u64;
} epoll_data_t;

struct epoll_event {
    uint32_t events;
    epoll_data_t data;
};

extern "C" {
int epoll_create1(int flags);
int epoll_ctl(int epfd, int op, int fd, struct epoll_event* event);
int epoll_wait(int epfd, struct epoll_event* events, int maxevents, int timeout);
int eventfd(unsigned int initval, int flags);
}

// ---------- sockets ----------
#define AF_UNSPEC 0
#define AF_INET 2
#define AF_INET6 10
#define SOCK_STREAM 1
#define SOCK_NONBLOCK 0x800
#define SOCK_CLOEXEC 0x80000
#define SOL_SOCKET 1
#define SO_REUSEADDR 2
#define SO_REUSEPORT 15
#define SO_KEEPALIVE 9
#define SO_LINGER 13
#define SO_RCVTIMEO 20
#define SHUT_RD 0
#define SHUT_WR 1
#define SHUT_RDWR 2
#define INADDR_LOOPBACK 0x7F000001u
#define IPPROTO_TCP 6
#define IPPROTO_IPV6 41
#define IPV6_V6ONLY 26
#define TCP_NODELAY 1
#define TCP_KEEPIDLE 4
#define TCP_KEEPINTVL 5
#define TCP_KEEPCNT 6
#define MSG_NOSIGNAL 0x4000
#define AI_PASSIVE 0x0001
#define NI_NUMERICHOST 1
#define NI_NUMERICSERV 2
#define NI_MAXHOST 1025
#define NI_MAXSERV 32

struct sockaddr { unsigned short sa_family; char sa_data[14]; };
struct sockaddr_storage { unsigned short ss_family; char __pad[126]; };

// <netinet/in.h>
struct in_addr { unsigned int s_addr; };
struct sockaddr_in {
    short          sin_family;
    unsigned short sin_port;
    struct in_addr sin_addr;
    char           sin_zero[8];
};
unsigned short htons(unsigned short v);
unsigned short ntohs(unsigned short v);
unsigned int   htonl(unsigned int v);
unsigned int   ntohl(unsigned int v);

// <sys/socket.h> SO_LINGER payload: l_onoff=1,l_linger=0 asks for RST on close.
struct linger { int l_onoff; int l_linger; };

struct iovec { void* iov_base; size_t iov_len; };

struct msghdr {
    void* msg_name;
    socklen_t msg_namelen;
    struct iovec* msg_iov;
    size_t msg_iovlen;
    void* msg_control;
    size_t msg_controllen;
    int msg_flags;
};

struct addrinfo {
    int ai_flags;
    int ai_family;
    int ai_socktype;
    int ai_protocol;
    socklen_t ai_addrlen;
    struct sockaddr* ai_addr;
    char* ai_canonname;
    struct addrinfo* ai_next;
};

extern "C" {
int socket(int domain, int type, int protocol);
int connect(int sockfd, const struct sockaddr* addr, socklen_t addrlen);
int bind(int sockfd, const struct sockaddr* addr, socklen_t addrlen);
int getsockname(int sockfd, struct sockaddr* addr, socklen_t* addrlen);
ssize_t send(int sockfd, const void* buf, size_t len, int flags);
int listen(int sockfd, int backlog);
int accept4(int sockfd, struct sockaddr* addr, socklen_t* addrlen, int flags);
int setsockopt(int sockfd, int level, int optname, const void* optval, socklen_t optlen);
int getaddrinfo(const char* node, const char* service, const struct addrinfo* hints, struct addrinfo** res);
void freeaddrinfo(struct addrinfo* res);
int getnameinfo(const struct sockaddr* addr, socklen_t addrlen, char* host, socklen_t hostlen,
                char* serv, socklen_t servlen, int flags);
ssize_t sendmsg(int sockfd, const struct msghdr* msg, int flags);
ssize_t recv(int sockfd, void* buf, size_t len, int flags);
int shutdown(int sockfd, int how);
int close(int fd);
ssize_t read(int fd, void* buf, size_t count);
ssize_t write(int fd, const void* buf, size_t count);
int getrlimit(int resource, struct rlimit* rlp);
}

#define RLIMIT_NOFILE 7
struct rlimit { unsigned long rlim_cur; unsigned long rlim_max; };

// unistd aliases the header needs
using ::read;
using ::write;
using ::close;
