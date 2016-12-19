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

  SocketServer(const std::string log_tag = "SocketServer") : SocketCommon(log_tag), listening_(false) {}

  void Close() override {
    SocketCommon::Close();
    listening_ = false;
  }

  int Listen(uint16_t server_port) {
    struct S_ADDRINFO *result;

    if (listening_) {
      return sock_fd_;
    }

    result = InitSocket(nullptr, std::to_string(server_port).c_str(), AI_PASSIVE);
    if (!result) {
      return -1;
    }

    if (!SetReuseAddr()) {
      return -1;
    }

    if (S_BIND(sock_fd_, S_SRC_ADDR(result), S_SRC_ADDRLEN(result)) == -1) {
      LogError(errno);
      S_FREEADDRINFO(result);
      return -1;
    }

    if (S_LISTEN(sock_fd_, 1024) == -1) {
      LogError(errno);
      S_FREEADDRINFO(result);
      return -1;
    }

    S_FREEADDRINFO(result);
    listening_ = true;
    LogInfo("now listening on port " + std::to_string(server_port));

    return sock_fd_;
  }

  std::unique_ptr<SocketCommon> Accept() {
    auto sock_fd = S_ACCEPT(sock_fd_, NULL, NULL);
    if (sock_fd < 0) {
      if (errno != EWOULDBLOCK) {
        LogError(errno);
      }

      return nullptr;
    }

    return std::unique_ptr<SocketCommon>(new SocketCommon(sock_fd, GetLogTag()));
  }

 protected:
  int GetListenSocket() const {
    return sock_fd_;
  }

 private:
  bool listening_;

  bool SetReuseAddr(bool reuse = true) {
    int val = reuse ? 1 : 0;
    return SetSockOpt(SOL_SOCKET, SO_REUSEADDR, val);
  }

};


#endif //SOCKET_SOCKET_SERVER_H
