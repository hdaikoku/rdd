//
// Created by hdaikoku on 15/11/10.
//


#include <iostream>
#include <memory>
#include <netdb.h>
#include <netinet/tcp.h>
#include <string.h>

#include "socket_common.h"

struct addrinfo *SocketCommon::InitSocket(const char *addr, const char *port, int flags) {
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

ssize_t SocketCommon::Write(int sock_fd, const void *buf, size_t len) const {
  size_t offset = 0;
  ssize_t ret = 0;

  while (offset < len) {
    ret = write(sock_fd, buf + offset, len - offset);
    if (ret > 0) {
      offset += ret;
    } else {
      perror("write");
      break;
    }
  }

  return ret;
}

ssize_t SocketCommon::WriteWithProbe(int sock_fd, const void *buf, size_t len) const {
  size_t len_ack = 0;

  if (Write(sock_fd, &len, sizeof(size_t)) < 0) {
    return -1;
  }

  if (Read(sock_fd, &len_ack, sizeof(size_t)) < 0) {
    return -1;
  }

  if (len != len_ack) {
    return -1;
  }

  return Write(sock_fd, buf, len);
}

ssize_t SocketCommon::Read(int sock_fd, void *buf, size_t len) const {
  size_t offset = 0;
  ssize_t ret = 0;

  while (offset < len) {
    ret = read(sock_fd, buf + offset, len - offset);
    if (ret > 0) {
      offset += ret;
    } else {
      perror("read");
      break;
    }
  }

  return ret;
}

std::unique_ptr<char[]> SocketCommon::ReadWithProbe(int sock_fd, size_t &len) const {
  if (Read(sock_fd, &len, sizeof(size_t)) < 0) {
    return nullptr;
  }

  std::unique_ptr<char[]> buf(new char[len]);

  if (Write(sock_fd, &len, sizeof(size_t)) < 0) {
    return nullptr;
  }

  if (Read(sock_fd, buf.get(), len) < 0) {
    return nullptr;
  }

  return buf;
}

bool SocketCommon::SetSockOpt() {
  int val;

  val = (1 << 19);
  if (setsockopt(sock_fd_, SOL_SOCKET, SO_SNDBUF, (void *) &val, sizeof(int)) != 0) {
    return false;
  }
  if (setsockopt(sock_fd_, SOL_SOCKET, SO_RCVBUF, (void *) &val, sizeof(int)) != 0) {
    return false;
  }

  val = 1;
  if (setsockopt(sock_fd_, IPPROTO_TCP, TCP_NODELAY, (void *) &val, sizeof(val)) != 0) {
    return false;
  }

  return true;
}
