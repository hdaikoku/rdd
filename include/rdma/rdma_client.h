//
// Created by hdaikoku on 15/11/10.
//

#ifndef RDMA_SERVER_CLIENT_RDMA_CLIENT_H
#define RDMA_SERVER_CLIENT_RDMA_CLIENT_H

#include <string>
#include <chrono>
#include <thread>
#include "rdma_common.h"

class RDMAClient: public RDMACommon {
 public:

  RDMAClient(const std::string &server_addr, int server_port)
      : server_addr_(server_addr), server_port_(server_port) { }

  int Connect() {
    struct rdma_addrinfo *result;

    auto start = std::chrono::steady_clock::now();
    auto timeout = std::chrono::minutes(1);
    while (true) {
      if (start + timeout < std::chrono::steady_clock::now()) {
        // timed-out
        return -1;
      }

      result = InitSocket(server_addr_.c_str(), std::to_string(server_port_).c_str(), NULL);
      if (!result) {
        return -1;
      }

      if (rconnect(sock_fd_, result->ai_dst_addr, result->ai_dst_len) == -1) {
        rclose(sock_fd_);
        rdma_freeaddrinfo(result);
        if (errno == ECONNREFUSED) {
          // the server is not ready yet.
          std::this_thread::sleep_for(std::chrono::milliseconds(1));
          continue;
        } else {
          // unrecoverable error.
          return -1;
        }
      }

      // successfully connected.
      break;
    }

    rdma_freeaddrinfo(result);

    return sock_fd_;
  }

  ssize_t Write(const void *buf, size_t len) const {
    return RDMACommon::Write(sock_fd_, buf, len);
  }

  ssize_t Read(void *buf, size_t len) const {
    return RDMACommon::Read(sock_fd_, buf, len);
  }

 private:
  std::string server_addr_;
  int server_port_;

};


#endif //RDMA_SERVER_CLIENT_RDMA_CLIENT_H
