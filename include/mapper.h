//
// Created by Harunobu Daikoku on 2015/11/20.
//

#ifndef SLAVERDD_MAPPER_H
#define SLAVERDD_MAPPER_H

#include <memory>
#include <tbb/tbb.h>
#include <unordered_map>
#include <vector>
#include <sparsehash/dense_hash_map>

template <typename NK, typename NV, typename K, typename V>
class Mapper {
 public:
  Mapper() { }
  virtual ~Mapper() { }

  virtual void Map(google::dense_hash_map<NK, std::vector<NV>> &kvs,
                   const K &key, const V &value) = 0;

};

template<typename NK, typename NV, typename K, typename V>
using CreateMapper = typename std::unique_ptr<Mapper<NK, NV, K, V>> (*)();

#endif //SLAVERDD_MAPPER_H
