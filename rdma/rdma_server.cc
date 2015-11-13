//
// Created by hdaikoku on 15/11/10.
//

#include <netdb.h>
#include "rdma_server.h"

bool RDMAServer::Listen() {
  struct rdma_addrinfo *result;

  result = InitSocket(nullptr, server_port_.c_str(), RAI_PASSIVE);
  if (!result) {
    return -1;
  }

  if (rbind(sock_fd_, result->ai_src_addr, result->ai_src_len) == -1) {
    perror("rbind");
    rdma_freeaddrinfo(result);
    return -1;
  }

  if (rlisten(sock_fd_, 10) == -1) {
    perror("rlisten");
    rdma_freeaddrinfo(result);
  }

  rdma_freeaddrinfo(result);

  return true;
}

int RDMAServer::Accept() {
  return raccept(sock_fd_, NULL, NULL);
}

bool RDMAServer::SetSockOpt() {
  if (!RDMACommon::SetSockOpt()) {
    return false;
  }

  int val = 1;
  return (rsetsockopt(sock_fd_, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(int)) == 0);
}
