//
// Created by Harunobu Daikoku on 2016/01/11.
//

#ifndef PROJECT_PAIRWISE_SHUFFLE_CLIENT_H
#define PROJECT_PAIRWISE_SHUFFLE_CLIENT_H

#include "block_manager.h"

class PairwiseShuffleClient {
 public:
  PairwiseShuffleClient(BlockManager &block_mgr) : block_mgr_(block_mgr) { }

  void Start(int server_id, const std::string &server_addr, int server_portz);

 private:
  BlockManager &block_mgr_;

  void PackBlocks(int server_id, msgpack::sbuffer &sbuf);
  void UnpackBlocks(int server_id, const char *buf, long len);
};

#endif //PROJECT_PAIRWISE_SHUFFLE_CLIENT_H
