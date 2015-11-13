//
// Created by hdaikoku on 15/11/10.
//

#ifndef SOCKET_SERVER_CLIENT_SOCKET_CLIENT_H
#define SOCKET_SERVER_CLIENT_SOCKET_CLIENT_H

#include <string>
#include "socket_common.h"

class SocketClient: public SocketCommon {
 public:

  SocketClient(const std::string &server_addr, const std::string &server_port)
      : server_addr_(server_addr), server_port_(server_port) { }

  int Connect();

 private:
  std::string server_addr_;
  std::string server_port_;
};


#endif //SOCKET_SERVER_CLIENT_SOCKET_CLIENT_H
