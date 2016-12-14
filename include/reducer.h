//
// Created by Harunobu Daikoku on 2015/11/20.
//

#ifndef SLAVERDD_REDUCER_H
#define SLAVERDD_REDUCER_H

#include <memory>
#include <vector>

template<typename K, typename V>
class Reducer {
 public:
  Reducer() { }
  virtual ~Reducer() { }

  virtual std::pair<K, V> Reduce(const K &key, const std::vector<V> &values) const = 0;
};

template<typename K, typename V>
using CreateReducer = typename std::unique_ptr<Reducer<K, V>> (*)();

#endif //SLAVERDD_REDUCER_H
