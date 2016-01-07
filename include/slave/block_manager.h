//
// Created by Harunobu Daikoku on 2015/12/24.
//

#ifndef OVERLAP_BLOCK_MANAGER_H
#define OVERLAP_BLOCK_MANAGER_H

#include <msgpack.hpp>
#include <tbb/mutex.h>
#include <tbb/concurrent_unordered_map.h>
#include <tbb/concurrent_queue.h>
#include <array>

using Block = std::pair<long, std::unique_ptr<char[]>>;

class BlockManager {
 public:

  BlockManager(int n_buffers)
      : finalized_(false), n_buffers_(n_buffers), buffers_(n_buffers) { }

  std::unique_ptr<char[]> GetBlock(int buffer_id, long &len);

  void PutBlock(int buffer_id, size_t len, std::unique_ptr<char[]> block);

  int GetNumOfBuffers() {
    return n_buffers_;
  }

  void Finalize();

 protected:
  std::vector<tbb::concurrent_queue<Block>> buffers_;
  int n_buffers_;
  bool finalized_;

};


#endif //OVERLAP_BLOCK_MANAGER_H
