//
// Created by hdaikoku on 15/11/10.
//

#ifndef SOCKET_SERVER_CLIENT_SOCKET_CLIENT_H
#define SOCKET_SERVER_CLIENT_SOCKET_CLIENT_H

#include <string>
#include <thread>

#include "worker/shuffle/socket/socket_common.h"

class SocketClient: public SocketCommon {
 public:
  SocketClient(const std::string &server_addr, int server_port)
      : server_addr_(server_addr), server_port_(server_port) { }

  int Connect() {
    struct addrinfo *result;

    auto start = std::chrono::steady_clock::now();
    auto timeout = std::chrono::minutes(1);
    while (true) {
      if (start + timeout < std::chrono::steady_clock::now()) {
        // timed-out
        return -1;
      }

      result = InitSocket(server_addr_.c_str(), std::to_string(server_port_).c_str(), 0);
      if (!result) {
        return -1;
      }

      if (connect(sock_fd_, result->ai_addr, result->ai_addrlen) == -1) {
        close(sock_fd_);
        freeaddrinfo(result);
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

    freeaddrinfo(result);

    return sock_fd_;
  }

  ssize_t Write(const void *buf, size_t len) const {
    return SocketCommon::Write(sock_fd_, buf, len);
  }

  ssize_t Read(void *buf, size_t len) const {
    return SocketCommon::Read(sock_fd_, buf, len);
  }

 private:
  std::string server_addr_;
  int server_port_;
};


#endif //SOCKET_SERVER_CLIENT_SOCKET_CLIENT_H
