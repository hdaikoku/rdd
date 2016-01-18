//
// Created by Harunobu Daikoku on 2016/01/11.
//

#ifndef PROJECT_PAIRWISE_SHUFFLE_CLIENT_H
#define PROJECT_PAIRWISE_SHUFFLE_CLIENT_H

#include "block_manager.h"

class PairwiseShuffleClient {
 public:
  PairwiseShuffleClient(int my_rank, BlockManager &block_mgr) : my_rank_(my_rank), block_mgr_(block_mgr) { }

  void Start(int server_id, const std::string &server_addr, int server_portz);

 private:
  BlockManager &block_mgr_;
  int my_rank_;

  void PackBlocks(int server_rank, msgpack::sbuffer &sbuf);
  void UnpackBlocks(const char *buf, size_t len);
};

#endif //PROJECT_PAIRWISE_SHUFFLE_CLIENT_H
