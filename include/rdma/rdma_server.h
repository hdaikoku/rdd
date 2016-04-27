//
// Created by hdaikoku on 15/11/10.
//

#ifndef RDMA_SERVER_CLIENT_RDMA_SERVER_H
#define RDMA_SERVER_CLIENT_RDMA_SERVER_H

#include "rdma_common.h"

class RDMAServer: public RDMACommon {
 public:

  RDMAServer(int server_port) : server_port_(server_port) { }

  bool Listen() {
    auto result = InitSocket(nullptr, std::to_string(server_port_).c_str(), RAI_PASSIVE);
    if (!result) {
      return false;
    }

    if (rbind(sock_fd_, result->ai_src_addr, result->ai_src_len) == -1) {
      perror("rbind");
      rdma_freeaddrinfo(result);
      return false;
    }

    if (rlisten(sock_fd_, 1024) == -1) {
      perror("rlisten");
      rdma_freeaddrinfo(result);
    }

    rdma_freeaddrinfo(result);

    return true;
  }

  int Accept(int fd) {
    return raccept(fd, NULL, NULL);
  }

  virtual bool SetSockOpt() override {
    if (!RDMACommon::SetSockOpt()) {
      return false;
    }

    int val = 1;
    return (rsetsockopt(sock_fd_, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(int)) == 0);
  }

  int GetListenSocket() const {
    return sock_fd_;
  }

 private:
  int server_port_;
};


#endif //RDMA_SERVER_CLIENT_RDMA_SERVER_H
