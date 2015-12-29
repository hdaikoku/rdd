//
// Created by hdaikoku on 15/11/10.
//

#ifndef SOCKET_SERVER_CLIENT_SOCKET_SERVER_H
#define SOCKET_SERVER_CLIENT_SOCKET_SERVER_H

#include <string>
#include "socket_common.h"

class SocketServer: public SocketCommon {
 public:

  SocketServer(const std::string &server_port) : server_port_(server_port) { }

  bool Listen();

  int Accept();

  virtual bool SetSockOpt() override;
 private:
  std::string server_port_;
};


#endif //SOCKET_SERVER_CLIENT_SOCKET_SERVER_H
