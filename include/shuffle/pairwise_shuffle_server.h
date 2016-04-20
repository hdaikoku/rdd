//
// Created by Harunobu Daikoku on 2016/01/11.
//

#ifndef PROJECT_PAIRWISE_SHUFFLE_SERVER_H
#define PROJECT_PAIRWISE_SHUFFLE_SERVER_H

#include "slave/block_manager.h"

class PairwiseShuffleServer {
 public:
  PairwiseShuffleServer(int my_rank, BlockManager &block_mgr) : my_rank_(my_rank), block_mgr_(block_mgr) { }

  void Start(const std::vector<int> &partition_ids, int port);

 private:
  BlockManager &block_mgr_;
  int my_rank_;

};

#endif //PROJECT_PAIRWISE_SHUFFLE_SERVER_H
