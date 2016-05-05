//
// Created by Harunobu Daikoku on 2016/04/11.
//

#ifndef FULLY_CONNECTED_FULLY_CONNECTED_CLIENT_H
#define FULLY_CONNECTED_FULLY_CONNECTED_CLIENT_H

#include <algorithm>
#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <sys/socket.h>
#include <netdb.h>
#include <poll.h>
#include <msgpack/sbuffer.hpp>
#include "slave/block_manager.h"
#include "socket/socket_client.h"

class FullyConnectedClient {
 public:
  FullyConnectedClient(const std::vector<std::pair<std::string, int>> &servers,
                       const std::vector<int> partition_ids,
                       BlockManager &block_mgr)
      : partition_ids_(partition_ids), block_mgr_(block_mgr) {
    for (const auto &server : servers) {
      clients_.emplace_back(new SocketClient(server.first, server.second));
    }
  }

  std::thread Dispatch() {
    return std::thread([this]() {
      this->Run();
    });
  }

 private:
  static const int kMinBackoff = 1;
  static const int kMaxBackoff = 1024;
  std::vector<std::unique_ptr<SocketClient>> clients_;
  std::vector<int> partition_ids_;
  BlockManager &block_mgr_;

  void Run();

};

#endif //FULLY_CONNECTED_FULLY_CONNECTED_CLIENT_H
