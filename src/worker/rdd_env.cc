//
// Created by Harunobu Daikoku on 2016/07/20.
//

#include "rdd_env.h"

RDDEnv &RDDEnv::GetInstance() {
  static RDDEnv rdd_env;
  return rdd_env;
}

BlockManager &RDDEnv::GetBlockManager() {
  return block_manager_;
}