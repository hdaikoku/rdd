//
// Created by Harunobu Daikoku on 2015/11/05.
//

#ifndef SLAVERDD_RDD_H
#define SLAVERDD_RDD_H

#include <msgpack.hpp>
#include <string>
#include <vector>

#include "worker/shuffle/block_manager.h"

class RDD {
 public:

  RDD() { }
  RDD(int num_partitions, int partition_id)
      : num_partitions_(num_partitions), partition_id_(partition_id) { }

  virtual ~RDD() { }

  virtual void Compute() = 0;

  virtual void PutBlocks(BlockManager &block_mgr) = 0;
  virtual void GetBlocks(BlockManager &block_mgr) = 0;

  virtual void Print() const = 0;

  int GetNumPartitions() const {
    return num_partitions_;
  }

  int GetPartitionID() const {
    return partition_id_;
  }

 protected:
  int num_partitions_;
  int partition_id_;

  virtual void Pack(std::vector<msgpack::sbuffer> &buffers) const = 0;
  virtual void Unpack(const char *buf, size_t len) = 0;

  inline std::string ToString(const long long int &s) const {
    return std::to_string(s);
  }

  inline std::string ToString(const std::string &s) const {
    return s;
  }

  template <typename T1, typename T2>
  inline std::string ToString(const std::pair<T1, T2> &p) const {
    return (p.first + ", " + p.second);
  }

};


#endif //SLAVERDD_RDD_H
