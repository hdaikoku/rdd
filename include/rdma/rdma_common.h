//
// Created by hdaikoku on 15/11/10.
//

#ifndef RDMA_SERVER_CLIENT_RDMA_SOCKET_H
#define RDMA_SERVER_CLIENT_RDMA_SOCKET_H

#include <cstring>
#include <iostream>
#include <fcntl.h>
#include <memory>
#include <netdb.h>
#include <rdma/rsocket.h>
#include <string>
#include <netinet/tcp.h>

#define CONNECTION_CLOSED -1
#define CONNECTION_ERROR -2

class RDMACommon {
 public:

  virtual ~RDMACommon() {
    rclose(sock_fd_);
  }

  int GetSockFd() const {
    return sock_fd_;
  }

  struct rdma_addrinfo *InitSocket(const char *addr, const char *port, int flags) {
    struct rdma_addrinfo hints, *result;

    std::memset(&hints, 0, sizeof(struct rdma_addrinfo));
    hints.ai_flags = flags | RAI_FAMILY;
    hints.ai_port_space = RDMA_PS_TCP;

    int error = rdma_getaddrinfo(const_cast<char *>(addr), const_cast<char *>(port), &hints, &result);
    if (error) {
      std::cerr << "rdma_getaddrinfo: "
          << (error == EAI_SYSTEM ? strerror(errno) : gai_strerror(error)) << std::endl;
      return nullptr;
    }

    if ((sock_fd_ = rsocket(result->ai_family, SOCK_STREAM, 0)) == -1) {
      perror("rsocket");
      rdma_freeaddrinfo(result);
      return nullptr;
    }

    if (!SetSockOpt()) {
      perror("rsetsockopt");
      rdma_freeaddrinfo(result);
      rclose(sock_fd_);
      return nullptr;
    }

    return result;
  }

  ssize_t Write(int sock_fd, const void *buf, size_t len) const {
    size_t offset = 0;
    ssize_t ret = 0;

    while (offset < len) {
      ret = rwrite(sock_fd, (char *) buf + offset, len - offset);
      if (ret > 0) {
        offset += ret;
      } else {
        perror("write");
        return -1;
      }
    }

    return ret;
  }

  ssize_t WriteSome(int sock_fd, const void *buf, size_t len) const {
    size_t offset = 0;

    while (offset < len) {
      auto sent = rwrite(sock_fd, (char *) buf + offset, len - offset);
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

  ssize_t Read(int sock_fd, void *buf, size_t len) const {
    ssize_t offset = 0;

    while (offset < len) {
      auto recvd = rread(sock_fd, (char *) buf + offset, len - offset);
      if (recvd > 0) {
        offset += recvd;
      } else {
        perror("read");
        return -1;
      }
    }

    return offset;
  }

  ssize_t ReadSome(int sock_fd, void *buf, size_t len) const {
    ssize_t offset = 0;

    while (offset < len) {
      auto recvd = rrecv(sock_fd, (char *) buf + offset, len - offset, 0);
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


  virtual bool SetSockOpt() {
    int val;

    val = 1;
    if (rsetsockopt(sock_fd_, IPPROTO_TCP, TCP_NODELAY, (void *) &val, sizeof(val)) != 0) {
      return false;
    }
    // optimize for bandwidth
    val = 0;
    if (rsetsockopt(sock_fd_, SOL_RDMA, RDMA_INLINE, &val, sizeof val) != 0) {
      return false;
    }

    return true;
  }

  static void SetNonBlocking(int fd, bool nonblocking = true) {
    rfcntl(fd, F_SETFL, rfcntl(fd, F_GETFL, 0) | nonblocking ? O_NONBLOCK : ~O_NONBLOCK);
  }

 protected:
  int sock_fd_;

};

#endif //RDMA_SERVER_CLIENT_RDMA_SOCKET_H
