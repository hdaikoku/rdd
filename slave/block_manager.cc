//
// Created by Harunobu Daikoku on 2015/12/28.
//

#include <iostream>
#include "slave/block_manager.h"

std::unique_ptr<char[]> BlockManager::GetBlock(int buffer_id, int32_t &len) {
  Block block(0, std::unique_ptr<char[]>(nullptr));
  if (!buffers_[buffer_id].try_pop(block) && !running_) {
    block.first = -1;
  }

  len = block.first;
  return std::move(block.second);
}

void BlockManager::PutBlock(int buffer_id, int32_t len, std::unique_ptr<char[]> block) {
  buffers_[buffer_id].push(Block(len, std::move(block)));
}

void BlockManager::PackBlocks(int partition_id, msgpack::sbuffer &sbuf, std::vector<std::unique_ptr<char[]>> &refs) {
  int32_t len = 0;
  while (true) {
    auto block = GetBlock(partition_id, len);
    if (len == -1) {
      break;
    }
    msgpack::pack(&sbuf, msgpack::type::raw_ref(block.get(), len));
    refs.push_back(std::move(block));
  }
}

void BlockManager::UnpackBlocks(int partition_id, const char *buf, size_t len) {
  size_t offset = 0;
  msgpack::unpacked unpacked;
  while (offset != len) {
    msgpack::unpack(&unpacked, buf, len, &offset);
    auto raw = unpacked.get().via.raw;
    std::unique_ptr<char[]> block(new char[raw.size]);
    memcpy(block.get(), raw.ptr, raw.size);
    PutBlock(partition_id, raw.size, std::move(block));
  }
}

int BlockManager::GroupPackBlocks(const std::vector<int> &partition_ids,
                                  msgpack::sbuffer &sbuf,
                                  std::vector<std::unique_ptr<char[]>> &refs) {
  int32_t len = 0;
  int num_no_more = 0;
  for (const auto &partition_id : partition_ids) {
    msgpack::pack(&sbuf, partition_id);
    while (true) {
      auto block = GetBlock(partition_id, len);
      if (len == -1) {
        num_no_more++;
        break;
      } else if (len == 0) {
        break;
      } else {
        msgpack::pack(&sbuf, msgpack::type::raw_ref(block.get(), len));
        refs.push_back(std::move(block));
      }
    }
  }

  if (refs.size() == 0) {
    return (num_no_more == partition_ids.size()) ? -1 : 0;
  }

  return sbuf.size();
}

void BlockManager::GroupUnpackBlocks(const char *buf, size_t len) {
  size_t offset = 0;
  int partition_id = 0;
  msgpack::unpacked unpacked;
  while (offset != len) {
    msgpack::unpack(&unpacked, buf, len, &offset);
    if (unpacked.get().type == msgpack::type::RAW) {
      auto raw = unpacked.get().via.raw;
      std::unique_ptr<char[]> block(new char[raw.size]);
      memcpy(block.get(), raw.ptr, raw.size);
      PutBlock(partition_id, raw.size, std::move(block));
    } else {
      partition_id = unpacked.get().as<int>();
    }
  }
}

int BlockManager::GetNumBuffers() const {
  return num_buffers_;
}

void BlockManager::Finalize() {
  running_ = false;
}
