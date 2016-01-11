//
// Created by Harunobu Daikoku on 2016/01/11.
//

#ifndef PROJECT_PAIRWISE_SHUFFLE_SERVER_H
#define PROJECT_PAIRWISE_SHUFFLE_SERVER_H

#include "block_manager.h"

class PairwiseShuffleServer {
 public:
  PairwiseShuffleServer(BlockManager &block_mgr) : block_mgr_(block_mgr) { }

  void Start(int client_id, int port);

 private:
  BlockManager &block_mgr_;

  void PackBlocks(int client_id, msgpack::sbuffer &sbuf);
  void UnpackBlocks(int client_id, const char *buf, long len);
};

#endif //PROJECT_PAIRWISE_SHUFFLE_SERVER_H
