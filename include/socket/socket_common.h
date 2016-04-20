//
// Created by hdaikoku on 15/11/10.
//

#ifndef SOCKET_SERVER_CLIENT_SOCKET_SOCKET_H
#define SOCKET_SERVER_CLIENT_SOCKET_SOCKET_H

#include <iostream>
#include <fcntl.h>
#include <memory>
#include <netinet/tcp.h>
#include <string>
#include <unistd.h>
#include <netdb.h>

#define CONNECTION_CLOSED -1
#define CONNECTION_ERROR -2

class SocketCommon {
 public:
  virtual ~SocketCommon() {
    close(sock_fd_);
  }

  struct addrinfo *InitSocket(const char *addr, const char *port, int flags) {
    struct addrinfo hints, *result;

    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_INET;
    hints.ai_flags = flags;

    int error = getaddrinfo(addr, port, &hints, &result);
    if (error) {
      std::cerr << "getaddrinfo: "
          << (error == EAI_SYSTEM ? strerror(errno) : gai_strerror(error)) << std::endl;
      return nullptr;
    }

    if ((sock_fd_ = socket(result->ai_family, SOCK_STREAM, 0)) == -1) {
      perror("socket");
      freeaddrinfo(result);
      return nullptr;
    }

    if (!SetSockOpt()) {
      perror("setsockopt");
      freeaddrinfo(result);
      close(sock_fd_);
      return nullptr;
    }

    return result;
  }

  ssize_t Write(int sock_fd, const void *buf, size_t len) const {
    size_t offset = 0;
    ssize_t ret = 0;

    while (offset < len) {
      ret = write(sock_fd, (char *) buf + offset, len - offset);
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
      auto sent = write(sock_fd, (char *) buf + offset, len - offset);
      if (sent < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
          // send buffer is full
          std::cerr << "buffer is full" << std::endl;
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
      auto recvd = read(sock_fd, (char *) buf + offset, len - offset);
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
      auto recvd = recv(sock_fd, (char *) buf + offset, len - offset, 0);
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
    if (setsockopt(sock_fd_, IPPROTO_TCP, TCP_NODELAY, (void *) &val, sizeof(val)) != 0) {
      return false;
    }

    return true;
  }

  static void SetNonBlocking(int fd, bool nonblocking = true) {
    fcntl(fd, F_SETFL, fcntl(fd, F_GETFL, 0) | nonblocking ? O_NONBLOCK : ~O_NONBLOCK);
  }

 protected:
  int sock_fd_;

};


#endif //SOCKET_SERVER_CLIENT_SOCKET_SOCKET_H
