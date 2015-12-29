//
// Created by Harunobu Daikoku on 2015/12/24.
//

#ifndef OVERLAP_SHUFFLE_CLIENT_H
#define OVERLAP_SHUFFLE_CLIENT_H

#include <thread>
#include <queue>
#include "socket/socket_client.h"
#include "block_manager.h"

class ShuffleClient {
 public:
  ShuffleClient(const std::vector<std::pair<std::string, std::string>> &servers, int my_rank, BlockManager &block_mgr_)
      : block_mgr_(block_mgr_), my_rank_(my_rank) {
    for (const auto &server : servers) {
      clients_queue_.push(SocketClient(server.first, server.second));
    }
  }

  std::thread Start() {
    return std::thread([=]() {
      FetchBlocks();
    });
  }

 private:
  BlockManager &block_mgr_;
  std::queue<SocketClient> clients_queue_;
  int my_rank_;

  bool FetchBlocks();
};


#endif //OVERLAP_SHUFFLE_CLIENT_H
