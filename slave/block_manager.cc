//
// Created by Harunobu Daikoku on 2015/12/28.
//

#include <iostream>
#include "block_manager.h"

std::unique_ptr<char[]> BlockManager::GetBlock(int buffer_id, long &len) {
  Block block(0, std::unique_ptr<char[]>(nullptr));
  if (!buffers_[buffer_id].try_pop(block) && finalized_) {
    block.first = -1;
  }

  len = block.first;
  return std::move(block.second);
}

void BlockManager::PutBlock(int buffer_id, size_t len, std::unique_ptr<char[]> block) {
  buffers_[buffer_id].push(Block(len, std::move(block)));
}

void BlockManager::Finalize() {
  finalized_ = true;
}
