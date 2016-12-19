//
// Created by Harunobu Daikoku on 2016/07/13.
//

#ifndef PROJECT_SOCKET_COMMON_H
#define PROJECT_SOCKET_COMMON_H

#include <cstring>
#include <errno.h>
#include <fcntl.h>
#include <iostream>
#include <memory>
#include <netdb.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include "logger.h"

#ifdef USE_RDMA
// rsocket-specific functions
#include <rdma/rsocket.h>

#define S_SOCKET(f, t, p)            rsocket(f, t, p)
#define S_CLOSE(f)                   rclose(f)
#define S_SHUTDOWN(f, h)             rshutdown(f, h)
#define S_WRITE(f, b, l)             rwrite(f, b, l)
#define S_READ(f, b, l)              rread(f, b, l)
#define S_FCNTL(s,c,p)               rfcntl(s, c, p)
#define S_ADDRINFO                   rdma_addrinfo
#define S_GETADDRINFO(a, p, h, r)    rdma_getaddrinfo(a, p, h, r)
#define S_FREEADDRINFO(r)            rdma_freeaddrinfo(r)
#define S_SETSOCKOPT(s, l, n, v, ol) rsetsockopt(s, l, n, v, ol)
#define S_GETSOCKOPT(s, l, n, v, ol) rgetsockopt(s, l, n, v, ol)
#define S_POLL(f, n, t)              rpoll(f, n, t)
#define S_GETPEERNAME(s, a, l)       rgetpeername(s, a, l)
#else
// BSD socket-specific functions
#define S_SOCKET(f, t, p)            socket(f, t, p)
#define S_CLOSE(f)                   close(f)
#define S_SHUTDOWN(f, h)             shutdown(f, h)
#define S_WRITE(f, b, l)             write(f, b, l)
#define S_READ(f, b, l)              read(f, b, l)
#define S_FCNTL(s, c, p)             fcntl(s, c, p)
#define S_ADDRINFO                   addrinfo
#define S_GETADDRINFO(a, p, h, r)    getaddrinfo(a, p, h, r)
#define S_FREEADDRINFO(r)            freeaddrinfo(r)
#define S_SETSOCKOPT(s, l, n, v, ol) setsockopt(s, l, n, v, ol)
#define S_GETSOCKOPT(s, l, n, v, ol) getsockopt(s, l, n, v, ol)
#define S_POLL(f, n, t)              poll(f, n, t)
#define S_GETPEERNAME(s, a, l)       getpeername(s, a, l)
#endif //USE_RDMA

#define CONNECTION_CLOSED -1
#define CONNECTION_ERROR -2

class SocketCommon : public Logger {
 public:

  SocketCommon(const std::string &log_tag = "SocketCommon") : Logger(log_tag), sock_fd_(-1) {}

  SocketCommon(int sock_fd, const std::string &log_tag = "SocketCommon") : Logger(log_tag), sock_fd_(sock_fd) {
    SetPeerName();
  }

  virtual ~SocketCommon() {
    Close();
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
      if (error == EAI_SYSTEM) {
        LogError(errno);
      } else {
        LogError(gai_strerror(error));
      }
      return nullptr;
    }

    if ((sock_fd_ = S_SOCKET(result->ai_family, SOCK_STREAM, 0)) == -1) {
      LogError("failed to initialize socket");
      S_FREEADDRINFO(result);
      return nullptr;
    }

    if (!SetTCPNoDelay()) {
      LogError("failed to set TCP_NODELAY");
      S_FREEADDRINFO(result);
      S_CLOSE(sock_fd_);
      return nullptr;
    }

#ifdef USE_RDMA
    // optimization for bandwidth
    int val = 0;
    if (!SetSockOpt(SOL_RDMA, RDMA_INLINE, val)) {
      LogError("failed to set RDMA_INLINE");
      S_FREEADDRINFO(result);
      S_CLOSE(sock_fd_);
      return nullptr;
    }
#endif //USE_RDMA

    return result;
  }

  virtual void Close() {
    S_SHUTDOWN(sock_fd_, SHUT_RDWR);
    S_CLOSE(sock_fd_);
  }

  ssize_t Write(const void *buf, size_t len) const {
    size_t offset = 0;
    ssize_t ret = 0;

    while (offset < len) {
      ret = S_WRITE(sock_fd_, (char *) buf + offset, len - offset);
      if (ret > 0) {
        offset += ret;
      } else {
        LogError(errno);
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
        LogError(errno);
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
        LogError(errno);
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
        LogError(errno);
        return CONNECTION_ERROR;
      } else if (recvd == 0) {
        // connection closed by client
        return CONNECTION_CLOSED;
      }

      offset += recvd;
    }

    return offset;
  }

  void SetNonBlocking(bool nonblocking = true) {
    if (nonblocking) {
      S_FCNTL(sock_fd_, F_SETFL, S_FCNTL(sock_fd_, F_GETFL, 0) | O_NONBLOCK);
    } else {
      S_FCNTL(sock_fd_, F_SETFL, S_FCNTL(sock_fd_, F_GETFL, 0) & ~O_NONBLOCK);
    }

  }

  const std::string &GetPeerAddr() const {
    return peer_addr_;
  }

  uint16_t GetPeerPort() const {
    return peer_port_;
  }

  std::string GetPeerNameAsString() const {
    return (peer_addr_ + ":" + std::to_string(peer_port_));
  }

  bool GetSockOpt(int level, int option_name, int &val) const {
    socklen_t len;
    return (S_GETSOCKOPT(sock_fd_, level, option_name, &val, &len) == 0);
  }

 protected:
  int sock_fd_;
  std::string peer_addr_;
  uint16_t peer_port_;

  bool SetTCPNoDelay(bool no_delay = true) {
    int val = no_delay ? 1 : 0;
    return SetSockOpt(IPPROTO_TCP, TCP_NODELAY, no_delay);
  }

  bool SetSockOpt(int level, int option_name, int val) {
    return (S_SETSOCKOPT(sock_fd_, level, option_name, &val, sizeof(val)) == 0);
  }

  bool SetPeerName() {
    struct sockaddr_in addr;
    socklen_t addr_len = sizeof(addr);

    if (S_GETPEERNAME(sock_fd_, (struct sockaddr *) &addr, &addr_len) != 0) {
      return false;
    }

    peer_addr_.assign(inet_ntoa(addr.sin_addr));
    peer_port_ = ntohs(addr.sin_port);

    return true;
  }
};

#endif //PROJECT_SOCKET_COMMON_H
