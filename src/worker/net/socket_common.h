//
// Created by Harunobu Daikoku on 2016/07/13.
//

#ifndef PROJECT_SOCKET_COMMON_H
#define PROJECT_SOCKET_COMMON_H

#include <cstring>
#include <errno.h>
#include <fcntl.h>
#include <memory>
#include <netdb.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#ifdef USE_RDMA
// rsocket-specific functions
#include <rdma/rsocket.h>

#define S_SOCKET(f, t, p)            rsocket(f, t, p)
#define S_CLOSE(f)                   rclose(f)
#define S_WRITE(f, b, l)             rwrite(f, b, l)
#define S_READ(f, b, l)              rread(f, b, l)
#define S_FCNTL(s,c,p)               rfcntl(s, c, p)
#define S_ADDRINFO                   rdma_addrinfo
#define S_GETADDRINFO(a, p, h, r)    rdma_getaddrinfo(a, p, h, r)
#define S_FREEADDRINFO(r)            rdma_freeaddrinfo(r)
#define S_SETSOCKOPT(s, l, n, v, ol) rsetsockopt(s, l, n, v, ol)
#define S_POLL(f, n, t)              rpoll(f, n, t)
#else
// BSD socket-specific functions
#define S_SOCKET(f, t, p)            socket(f, t, p)
#define S_CLOSE(f)                   close(f)
#define S_WRITE(f, b, l)             write(f, b, l)
#define S_READ(f, b, l)              read(f, b, l)
#define S_FCNTL(s, c, p)               fcntl(s, c, p)
#define S_ADDRINFO                   addrinfo
#define S_GETADDRINFO(a, p, h, r)    getaddrinfo(a, p, h, r)
#define S_FREEADDRINFO(r)            freeaddrinfo(r)
#define S_SETSOCKOPT(s, l, n, v, ol) setsockopt(s, l, n, v, ol)
#define S_POLL(f, n, t)              poll(f, n, t)
#endif //USE_RDMA

#define CONNECTION_CLOSED -1
#define CONNECTION_ERROR -2

class SocketCommon {
 public:

  SocketCommon() : sock_fd_(-1) {}

  SocketCommon(int sock_fd) : sock_fd_(sock_fd) {}

  virtual ~SocketCommon() {
    S_CLOSE(sock_fd_);
  }

  int GetSockFD() const {
    return sock_fd_;
  }

  struct S_ADDRINFO *InitSocket(const char *addr, const char *port, int flags) {
    struct S_ADDRINFO hints, *result;

    std::memset(&hints, 0, sizeof(struct S_ADDRINFO));
#ifdef USE_RDMA
    hints.ai_flags = flags | RAI_FAMILY;
    hints.ai_port_space = RDMA_PS_TCP;
#else
    hints.ai_family = AF_INET;
    hints.ai_flags = flags;
#endif //USE_RDMA

    int error = S_GETADDRINFO(const_cast<char *>(addr), const_cast<char *>(port), &hints, &result);
    if (error) {
      std::cerr << "getaddrinfo: "
                << (error == EAI_SYSTEM ? strerror(errno) : gai_strerror(error)) << std::endl;
      return nullptr;
    }

    if ((sock_fd_ = S_SOCKET(result->ai_family, SOCK_STREAM, 0)) == -1) {
      perror("socket");
      S_FREEADDRINFO(result);
      return nullptr;
    }

    if (!SetSockOpts()) {
      perror("setsockopt");
      S_FREEADDRINFO(result);
      close(sock_fd_);
      return nullptr;
    }

    return result;
  }

  ssize_t Write(const void *buf, size_t len) const {
    size_t offset = 0;
    ssize_t ret = 0;

    while (offset < len) {
      ret = S_WRITE(sock_fd_, (char *) buf + offset, len - offset);
      if (ret > 0) {
        offset += ret;
      } else {
        perror("write");
        return -1;
      }
    }

    return ret;
  }

  ssize_t WriteSome(const void *buf, size_t len) const {
    size_t offset = 0;

    while (offset < len) {
      auto sent = S_WRITE(sock_fd_, (char *) buf + offset, len - offset);
      if (sent < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
          // send buffer is full
          break;
        }
        // some errors
        perror("write");
        return -1;
      } else {
        offset += sent;
      }
    }

    return offset;
  }

  ssize_t Read(void *buf, size_t len) const {
    ssize_t offset = 0;

    while (offset < len) {
      auto recvd = S_READ(sock_fd_, (char *) buf + offset, len - offset);
      if (recvd > 0) {
        offset += recvd;
      } else {
        perror("read");
        return -1;
      }
    }

    return offset;
  }

  ssize_t ReadSome(void *buf, size_t len) const {
    ssize_t offset = 0;

    while (offset < len) {
      auto recvd = S_READ(sock_fd_, (char *) buf + offset, len - offset);
      if (recvd < 0) {
        if (errno == EWOULDBLOCK || errno == EAGAIN) {
          // no more data to receive
          break;
        }
        // something went wrong
        return CONNECTION_ERROR;
      } else if (recvd == 0) {
        // connection closed by client
        return CONNECTION_CLOSED;
      }

      offset += recvd;
    }

    return offset;
  }

  virtual bool SetSockOpts() {
    int val;

    val = 1;
    if (S_SETSOCKOPT(sock_fd_, IPPROTO_TCP, TCP_NODELAY, (void *) &val, sizeof(val)) != 0) {
      return false;
    }
#ifdef USE_RDMA
    // optimize for bandwidth
    val = 0;
    if (S_SETSOCKOPT(sock_fd_, SOL_RDMA, RDMA_INLINE, &val, sizeof val) != 0) {
      return false;
    }
#endif

    return true;
  }

  void SetNonBlocking(bool nonblocking = true) {
    S_FCNTL(sock_fd_, F_SETFL, S_FCNTL(sock_fd_, F_GETFL, 0) | nonblocking ? O_NONBLOCK : ~O_NONBLOCK);
  }

 protected:
  int sock_fd_;

};

#endif //PROJECT_SOCKET_COMMON_H
