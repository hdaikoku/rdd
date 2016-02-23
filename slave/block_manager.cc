//
// Created by Harunobu Daikoku on 2015/12/28.
//

#include <iostream>
#include "slave/block_manager.h"

std::unique_ptr<char[]> BlockManager::GetBlock(int buffer_id, int32_t &len) {
  Block block(0, std::unique_ptr<char[]>(nullptr));
  if (!buffers_[buffer_id].try_pop(block) && finalized_) {
    block.first = -1;
  }

  len = block.first;
  return std::move(block.second);
}

void BlockManager::PutBlock(int buffer_id, int32_t len, std::unique_ptr<char[]> block) {
  buffers_[buffer_id].push(Block(len, std::move(block)));
}


//void BlockManager::PackBlocks(int partition_id, msgpack::sbuffer &sbuf, std::vector<std::unique_ptr<char[]>> &refs) {
//  long len = 0;
//  while (true) {
//    auto block = GetBlock(partition_id, len);
//    if (len == -1) {
//      break;
//    }
//    msgpack::pack(&sbuf, partition_id);
//    msgpack::pack(&sbuf, msgpack::type::raw_ref(block.get(), len));
//    refs.push_back(std::move(block));
//  }
//}
//
//void BlockManager::UnpackBlocks(const char *buf, int32_t len) {
//  int32_t offset = 0;
//  msgpack::unpacked unpacked;
//  while (offset != len) {
//    msgpack::unpack(&unpacked, buf, len, &offset);
//    int partition_id = unpacked.get().as<int>();
//
//    msgpack::unpack(&unpacked, buf, len, &offset);
//    auto raw = unpacked.get().via.raw;
//    std::unique_ptr<char[]> block(new char[raw.size]);
//    memcpy(block.get(), raw.ptr, raw.size);
//    PutBlock(partition_id, raw.size, std::move(block));
//  }
//}

int BlockManager::GetNumBuffers() const {
  return n_buffers_;
}

void BlockManager::Finalize() {
  finalized_ = true;
}
