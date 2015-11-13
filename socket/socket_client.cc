//
// Created by hdaikoku on 15/11/10.
//

#include <netdb.h>

#include "socket_client.h"

int SocketClient::Connect() {
  struct addrinfo *result;

  result = InitSocket(server_addr_.c_str(), server_port_.c_str(), NULL);
  if (!result) {
    return -1;
  }

  if (connect(sock_fd_, result->ai_addr, result->ai_addrlen) == -1) {
    perror("connect");
    close(sock_fd_);
    freeaddrinfo(result);
    return -1;
  }

  freeaddrinfo(result);

  return sock_fd_;
}
