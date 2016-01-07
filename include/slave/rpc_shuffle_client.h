//
// Created by Harunobu Daikoku on 2016/01/06.
//

#ifndef PROJECT_RPC_SHUFFLE_CLIENT_H
#define PROJECT_RPC_SHUFFLE_CLIENT_H

#include <jubatus/msgpack/rpc/session_pool.h>
#include <thread>
#include "block_manager.h"

class RPCShuffleClient {
 public:
  RPCShuffleClient(const std::vector<std::pair<std::string, int>> &servers, int my_rank, BlockManager &block_mgr_)
      : servers_(servers.begin(), servers.end()), block_mgr_(block_mgr_), my_rank_(my_rank) { }

  std::thread Start() {
    return std::thread([=]() {
      FetchBlocks();
    });
  }

 private:
  BlockManager &block_mgr_;
  msgpack::rpc::session_pool sp_;
  std::deque<std::pair<std::string, int>> servers_;
  int my_rank_;

  bool FetchBlocks();

};

#endif //PROJECT_RPC_SHUFFLE_CLIENT_H
