//
// Created by Harunobu Daikoku on 2015/11/05.
//

#ifndef SLAVERDD_RDD_H
#define SLAVERDD_RDD_H

#include <msgpack.hpp>
#include <string>
#include <vector>
#include "worker/block_manager.h"

class RDD {
 public:
  RDD() { }
  RDD(int n_partitions, int partition_id) : n_partitions_(n_partitions), partition_id_(partition_id) { }
  virtual ~RDD() { }

  virtual void Compute() = 0;

  virtual void Print() = 0;

  int GetNumPartitions() const;

  int GetPartitionID() const;

 protected:
  int n_partitions_;
  int partition_id_;

  void *LoadLib(const std::string &dl_filename);
  void *LoadFunc(void *handle, const std::string &func_name);

  virtual void Pack(std::vector<msgpack::sbuffer> &buffers) const = 0;
  virtual void Unpack(const char *buf, size_t len) = 0;

  virtual void PutBlocks(BlockManager &block_mgr) = 0;
  virtual void GetBlocks(BlockManager &block_mgr) = 0;

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
