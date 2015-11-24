//
// Created by hdaikoku on 15/11/10.
//

#include <netdb.h>

#include "socket/socket_server.h"

bool SocketServer::Listen() {
  struct addrinfo *result;

  result = InitSocket(nullptr, server_port_.c_str(), AI_PASSIVE);
  if (!result) {
    return false;
  }

  if (bind(sock_fd_, result->ai_addr, result->ai_addrlen) == -1) {
    perror("bind");
    freeaddrinfo(result);
    return false;
  }

  if (listen(sock_fd_, 10) == -1) {
    perror("listen");
    freeaddrinfo(result);
  }

  freeaddrinfo(result);

  return true;
}

int SocketServer::Accept() {
  return accept(sock_fd_, NULL, NULL);
}

bool SocketServer::SetSockOpt() {
  if (!SocketCommon::SetSockOpt()) {
    return false;
  }

  int val = 1;
  return (setsockopt(sock_fd_, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(int)) == 0);
}
