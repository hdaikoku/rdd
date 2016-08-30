//
// Created by Harunobu Daikoku on 2016/07/26.
//

#ifndef SOCKET_SOCKET_SERVER_H
#define SOCKET_SOCKET_SERVER_H

#include <string>

#include "socket_common.h"

#ifdef USE_RDMA
// rsocket-specific functions
#define S_BIND(s, a, l)   rbind(s, a, l)
#define S_LISTEN(s, b)    rlisten(s, b)
#define S_ACCEPT(s, a, l) raccept(s, a, l)
#define S_SRC_ADDR(a)     a->ai_src_addr
#define S_SRC_ADDRLEN(a)  a->ai_src_len
#else
// BSD socket-specific functions
#define S_BIND(s, a, l)   bind(s, a, l)
#define S_LISTEN(s, b)    listen(s, b)
#define S_ACCEPT(s, a, l) accept(s, a, l)
#define S_SRC_ADDR(a)     a->ai_addr
#define S_SRC_ADDRLEN(a)  a->ai_addrlen
#endif

class SocketServer: public SocketCommon {
 public:

  SocketServer(const std::string &server_port) : server_port_(server_port) {}

  int Listen() {
    struct S_ADDRINFO *result;

    result = InitSocket(nullptr, server_port_.c_str(), AI_PASSIVE);
    if (!result) {
      return -1;
    }

    if (S_BIND(sock_fd_, S_SRC_ADDR(result), S_SRC_ADDRLEN(result)) == -1) {
      perror("bind");
      S_FREEADDRINFO(result);
      return -1;
    }

    if (S_LISTEN(sock_fd_, 1024) == -1) {
      perror("listen");
      S_FREEADDRINFO(result);
      return -1;
    }

    S_FREEADDRINFO(result);

    return sock_fd_;
  }

  std::unique_ptr<SocketCommon> Accept() {
    auto sock_fd = S_ACCEPT(sock_fd_, NULL, NULL);
    if (sock_fd < 0) {
      if (errno != EWOULDBLOCK) {
        perror("accept");
      }

      return nullptr;
    }

    return std::unique_ptr<SocketCommon>(new SocketCommon(sock_fd));
  }

  virtual bool SetSockOpts() override {
    if (!SocketCommon::SetSockOpts()) {
      return false;
    }

    int val = 1;
    return (S_SETSOCKOPT(sock_fd_, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(int)) == 0);
  }

 private:
  std::string server_port_;

};


#endif //SOCKET_SOCKET_SERVER_H
