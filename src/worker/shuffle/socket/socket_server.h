//
// Created by hdaikoku on 15/11/10.
//

#ifndef SOCKET_SERVER_CLIENT_SOCKET_SERVER_H
#define SOCKET_SERVER_CLIENT_SOCKET_SERVER_H

#include <string>
#include <sys/socket.h>
#include <netdb.h>

#include "worker/shuffle/socket/socket_common.h"

class SocketServer: public SocketCommon {
 public:

  SocketServer(int server_port) : server_port_(server_port) { }

  bool Listen() {
    struct addrinfo *result;

    result = InitSocket(nullptr, std::to_string(server_port_).c_str(), AI_PASSIVE);
    if (!result) {
      return false;
    }

    if (bind(sock_fd_, result->ai_addr, result->ai_addrlen) == -1) {
      perror("bind");
      freeaddrinfo(result);
      return false;
    }

    if (listen(sock_fd_, 1024) == -1) {
      perror("listen");
      freeaddrinfo(result);
    }

    freeaddrinfo(result);

    return true;
  }

  static int Accept(int fd) {
    return accept(fd, NULL, NULL);
  }

  virtual bool SetSockOpt() override {
    if (!SocketCommon::SetSockOpt()) {
      return false;
    }

    int val = 1;
    return (setsockopt(sock_fd_, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(int)) == 0);
  }

  int GetListenSocket() const {
    return sock_fd_;
  }

 private:
  int server_port_;
};


#endif //SOCKET_SERVER_CLIENT_SOCKET_SERVER_H
