//
// Created by hdaikoku on 15/11/10.
//

#ifndef SOCKET_SERVER_CLIENT_SOCKET_SOCKET_H
#define SOCKET_SERVER_CLIENT_SOCKET_SOCKET_H

#include <memory>
#include <string>
#include <unistd.h>

class SocketCommon {
 public:
  virtual ~SocketCommon() {
    close(sock_fd_);
  }

  struct addrinfo *InitSocket(const char *addr, const char *port, int flags);

  ssize_t Write(int sock_fd, const void *buf, size_t len) const;
  ssize_t WriteWithHeader(int sock_fd, const void *buf, size_t len) const;

  ssize_t Read(int sock_fd, void *buf, size_t len) const;
  std::unique_ptr<char[]> ReadWithHeader(int sock_fd, size_t &len) const;

  virtual bool SetSockOpt();

 protected:
  int sock_fd_;

};


#endif //SOCKET_SERVER_CLIENT_SOCKET_SOCKET_H
