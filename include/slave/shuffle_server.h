//
// Created by Harunobu Daikoku on 2015/12/24.
//

#ifndef OVERLAP_SHUFFLE_SERVER_H
#define OVERLAP_SHUFFLE_SERVER_H

#include "socket/socket_server.h"
#include "block_manager.h"
#include <tbb/tbb.h>
#include <thread>

class ShuffleServer {
 public:

  ShuffleServer(const std::string &server_port, BlockManager &block_mgr_)
      : block_mgr_(block_mgr_), server_(server_port) {
    n_dests_ = block_mgr_.GetNumOfBuffers() - 1;
  }

  void Dispatch();

  std::thread Start() {
    return std::thread([=] { Dispatch(); });
  }

 private:
  BlockManager &block_mgr_;
  SocketServer server_;
  int n_dests_;
};


#endif //OVERLAP_SHUFFLE_SERVER_H
