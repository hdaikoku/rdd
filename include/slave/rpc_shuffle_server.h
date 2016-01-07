//
// Created by Harunobu Daikoku on 2016/01/06.
//

#ifndef PROJECT_RPCSHUFFLESERVER_H
#define PROJECT_RPCSHUFFLESERVER_H

#include <jubatus/msgpack/rpc/server.h>
#include "block_manager.h"

class RPCShuffleServer: public msgpack::rpc::server::base {

 public:
  RPCShuffleServer(BlockManager &block_mgr) : block_mgr_(block_mgr), n_finished_(0) {
    n_clients_ = block_mgr_.GetNumOfBuffers() - 1;
  }

  virtual void dispatch(msgpack::rpc::request req) override;

 private:
  BlockManager &block_mgr_;
  int n_clients_;
  int n_finished_;
};

#endif //PROJECT_RPCSHUFFLESERVER_H