//
// Created by hdaikoku on 15/11/10.
//

#include <netdb.h>
#include "rdma_client.h"

int RDMAClient::Connect() {
  struct rdma_addrinfo *result;

  result = InitSocket(server_addr_.c_str(), server_port_.c_str(), NULL);
  if (!result) {
    return -1;
  }

  if (rconnect(sock_fd_, result->ai_dst_addr, result->ai_dst_len) == -1) {
    perror("rconnect");
    rclose(sock_fd_);
    rdma_freeaddrinfo(result);
    return -1;
  }

  rdma_freeaddrinfo(result);

  return sock_fd_;
}
