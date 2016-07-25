//
// Created by Harunobu Daikoku on 2015/12/24.
//

#ifndef OVERLAP_BLOCK_MANAGER_H
#define OVERLAP_BLOCK_MANAGER_H

#include <msgpack.hpp>
#include <tbb/concurrent_queue.h>

using Block = std::pair<int32_t, std::unique_ptr<char[]>>;

class BlockManager {
 public:

  BlockManager() : running_(true) {}

  BlockManager(int num_buffers)
      : running_(true), num_buffers_(num_buffers) {}

  std::unique_ptr<char[]> GetBlock(int buffer_id, int32_t &len);

  void PutBlock(int buffer_id, int32_t len, std::unique_ptr<char[]> block);

  void PackBlocks(int partition_id, msgpack::sbuffer &sbuf, std::vector<std::unique_ptr<char[]>> &refs);

  void UnpackBlocks(int partition_id, const char *buf, size_t len);

  int GroupPackBlocks
      (const std::vector<int> &partition_ids, msgpack::sbuffer &sbuf, std::vector<std::unique_ptr<char[]>> &refs);

  void GroupUnpackBlocks(const char *buf, size_t len);

  void SetNumBuffers(int num_buffers);

  int GetNumBuffers() const;

  void Finalize();

 protected:
  std::unordered_map<int, tbb::concurrent_queue<Block>> buffers_;
  int num_buffers_;
  bool running_;

};


#endif //OVERLAP_BLOCK_MANAGER_H
