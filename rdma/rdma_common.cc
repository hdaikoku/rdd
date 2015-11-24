//
// Created by hdaikoku on 15/11/10.
//

#include <iostream>
#include <memory>
#include <netdb.h>
#include <netinet/tcp.h>
#include <string.h>
#include "rdma/rdma_common.h"

struct rdma_addrinfo *RDMACommon::InitSocket(const char *addr, const char *port, int flags) {
  struct rdma_addrinfo hints, *result;

  memset(&hints, 0, sizeof(struct rdma_addrinfo));
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

ssize_t RDMACommon::Write(int sock_fd, const void *buf, size_t len) const {
  size_t offset = 0;
  ssize_t ret = 0;

  while (offset < len) {
    ret = rwrite(sock_fd, buf + offset, len - offset);
    if (ret > 0) {
      offset += ret;
    } else {
      perror("rwrite");
      break;
    }
  }

  return ret;
}

ssize_t RDMACommon::WriteWithProbe(int sock_fd, const void *buf, size_t len) const {
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

ssize_t RDMACommon::Read(int sock_fd, void *buf, size_t len) const {
  size_t offset = 0;
  ssize_t ret = 0;

  while (offset < len) {
    ret = rread(sock_fd, buf + offset, len - offset);
    if (ret > 0) {
      offset += ret;
    } else {
      perror("rread");
      break;
    }
  }

  return ret;
}

std::unique_ptr<char[]> RDMACommon::ReadWithProbe(int sock_fd, size_t &len) const {
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

bool RDMACommon::SetSockOpt() {
  int val;

  val = (1 << 19);
  if (rsetsockopt(sock_fd_, SOL_SOCKET, SO_SNDBUF, (void *) &val, sizeof(int)) != 0) {
    return false;
  }
  if (rsetsockopt(sock_fd_, SOL_SOCKET, SO_RCVBUF, (void *) &val, sizeof(int)) != 0) {
    return false;
  }

  val = 1;
  if (rsetsockopt(sock_fd_, IPPROTO_TCP, TCP_NODELAY, (void *) &val, sizeof(val)) != 0) {
    return false;
  }
//  rsetsockopt(sock_fd_, SOL_RDMA, RDMA_IOMAPSIZE, (void *) &val, sizeof val);

  return true;
}
