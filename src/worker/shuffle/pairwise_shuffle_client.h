//
// Created by Harunobu Daikoku on 2016/01/11.
//

#ifndef PROJECT_PAIRWISE_SHUFFLE_CLIENT_H
#define PROJECT_PAIRWISE_SHUFFLE_CLIENT_H

#include "worker/rdd_env.h"
#include "worker/shuffle/block_manager.h"

class PairwiseShuffleClient {
 public:
  PairwiseShuffleClient(int my_rank) : my_rank_(my_rank), block_mgr_(RDDEnv::GetInstance().GetBlockManager()) {}

  void Start(const std::vector<int> &partition_ids, const std::string &server_addr, const std::string &server_port);

 private:
  BlockManager &block_mgr_;
  int my_rank_;

};

#endif //PROJECT_PAIRWISE_SHUFFLE_CLIENT_H
