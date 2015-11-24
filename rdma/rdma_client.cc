//
// Created by hdaikoku on 15/11/10.
//

#include <fcntl.h>
#include <netdb.h>
#include "rdma/rdma_client.h"

int RDMAClient::Connect() {
  struct rdma_addrinfo *result;
  int res;

  result = InitSocket(server_addr_.c_str(), server_port_.c_str(), NULL);
  if (!result) {
    return -1;
  }

  int arg = fcntl(sock_fd_, F_GETFL, NULL);
  arg |= O_NONBLOCK;
  fcntl(sock_fd_, F_SETFL, arg);
  fd_set set;
  struct timeval timeout;

  res = rconnect(sock_fd_, result->ai_dst_addr, result->ai_dst_len);
  if (res < 0) {
    if (errno == EINPROGRESS) {
      timeout.tv_sec = 3;
      timeout.tv_usec = 0;
      FD_ZERO(&set);
      FD_SET(sock_fd_, &set);
      if (rselect(sock_fd_ + 1, NULL, &set, NULL, &timeout) > 0) {
        socklen_t lon = sizeof(int);
        int valopt;
        rgetsockopt(sock_fd_, SOL_SOCKET, SO_ERROR, &valopt, &lon);
        if (valopt) {
          rdma_freeaddrinfo(result);
          rclose(sock_fd_);
          return -1;
        }
      } else {
        rdma_freeaddrinfo(result);
        rclose(sock_fd_);
        return -1;
      }
    } else {
      rdma_freeaddrinfo(result);
      rclose(sock_fd_);
      return -1;
    }

  }

  arg = fcntl(sock_fd_, F_GETFL, NULL);
  arg &= (~O_NONBLOCK);
  fcntl(sock_fd_, F_SETFL, arg);

  rdma_freeaddrinfo(result);

  return sock_fd_;
}
