//
// Created by hdaikoku on 15/11/10.
//

#ifndef RDMA_SERVER_CLIENT_RDMA_SOCKET_H
#define RDMA_SERVER_CLIENT_RDMA_SOCKET_H

#include <memory>
#include <rdma/rsocket.h>
#include <string>
#include <unistd.h>

class RDMACommon {
 public:
  virtual ~RDMACommon() {
    rclose(sock_fd_);
  }

  struct rdma_addrinfo *InitSocket(const char *addr, const char *port, int flags);
  ssize_t Write(int sock_fd, const void *buf, size_t len) const;
  ssize_t WriteWithProbe(int sock_fd, const void *buf, size_t len) const;
  ssize_t WriteWithHeader(int sock_fd, const void *buf, size_t len) const;
  ssize_t Read(int sock_fd, void *buf, size_t len) const;
  std::unique_ptr<char[]> ReadWithProbe(int sock_fd, size_t &len) const;
  std::unique_ptr<char[]> ReadWithHeader(int sock_fd, size_t &len) const;

  virtual bool SetSockOpt();

 protected:
  int sock_fd_;

};


#endif //RDMA_SERVER_CLIENT_RDMA_SOCKET_H
