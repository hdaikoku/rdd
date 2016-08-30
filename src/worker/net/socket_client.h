//
// Created by Harunobu Daikoku on 2016/07/26.
//

#ifndef SOCKET_SOCKET_CLIENT_H
#define SOCKET_SOCKET_CLIENT_H

#include <chrono>
#include <string>
#include <thread>

#include "socket_common.h"

#ifdef USE_RDMA
// rdma-specific functions
#define S_CONNECT(s, a, l) rconnect(s, a, l)
#define S_DST_ADDR(a)      a->ai_dst_addr
#define S_DST_ADDRLEN(a)   a->ai_dst_len
#else
// BSD socket-specific functions
#define S_CONNECT(s, a, l) connect(s, a, l)
#define S_DST_ADDR(a)      a->ai_addr
#define S_DST_ADDRLEN(a)   a->ai_addrlen
#endif //USE_RDMA

class SocketClient: public SocketCommon {
 public:
  SocketClient(const std::string &server_addr, const std::string &server_port)
      : server_addr_(server_addr), server_port_(server_port) {}

  int Connect(int timeout_minutes = 1) {
    struct S_ADDRINFO *result;

    auto start = std::chrono::steady_clock::now();
    auto timeout = std::chrono::minutes(timeout_minutes);
    while (true) {
      if (start + timeout < std::chrono::steady_clock::now()) {
        // timed-out
        return -1;
      }

      result = InitSocket(server_addr_.c_str(), server_port_.c_str(), 0);
      if (!result) {
        return -1;
      }

      if (S_CONNECT(sock_fd_, S_DST_ADDR(result), S_DST_ADDRLEN(result)) == -1) {
        S_CLOSE(sock_fd_);
        S_FREEADDRINFO(result);
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

    S_FREEADDRINFO(result);

    return sock_fd_;
  }

 private:
  std::string server_addr_;
  std::string server_port_;
};

#endif //SOCKET_SOCKET_CLIENT_H
