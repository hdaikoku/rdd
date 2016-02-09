//
// Created by Harunobu Daikoku on 2015/11/05.
//

#ifndef SLAVERDD_RDD_H
#define SLAVERDD_RDD_H

#include <msgpack.hpp>
#include <string>
#include <slave/block_manager.h>
#include <vector>

class RDD {
 public:
  RDD() { }
  virtual ~RDD() { }

  virtual void Compute() = 0;

  virtual void Print() = 0;

 protected:
  void *LoadLib(const std::string &dl_filename);
  void *LoadFunc(void *handle, const std::string &func_name);

  virtual void Pack(std::vector<msgpack::sbuffer> &buffers) const = 0;
  virtual void Unpack(const char *buf, size_t len) = 0;

  virtual void PutBlocks(BlockManager &block_mgr) = 0;
  virtual void GetBlocks(BlockManager &block_mgr, int my_rank) = 0;

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
