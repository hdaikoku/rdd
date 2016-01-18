//
// Created by Harunobu Daikoku on 2016/01/11.
//

#ifndef PROJECT_PAIRWISE_SHUFFLE_SERVER_H
#define PROJECT_PAIRWISE_SHUFFLE_SERVER_H

#include "block_manager.h"

class PairwiseShuffleServer {
 public:
  PairwiseShuffleServer(int my_rank, BlockManager &block_mgr) : my_rank_(my_rank), block_mgr_(block_mgr) { }

  void Start(int client_id, int port);

 private:
  BlockManager &block_mgr_;
  int my_rank_;

  void PackBlocks(int client_rank, msgpack::sbuffer &sbuf);
  void UnpackBlocks(const char *buf, size_t len);
};

#endif //PROJECT_PAIRWISE_SHUFFLE_SERVER_H
