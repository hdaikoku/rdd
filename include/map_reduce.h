//
// Created by Harunobu Daikoku on 2015/11/07.
//

#ifndef SLAVERDD_MAP_REDUCE_H
#define SLAVERDD_MAP_REDUCE_H

#include <vector>
#include <unordered_map>

template<typename K, typename V, typename IV>
class MapReduce {
 public:
  MapReduce() { }
  virtual ~MapReduce() { }

  virtual void Map(std::unordered_map<K, std::vector<V>> &kvs, const IV &value) = 0;
  virtual std::pair<K, V> Reduce(const K &key, const std::vector<V> &values) = 0;

};

template<typename K, typename V, typename IV>
using CreateMapReduce = typename std::unique_ptr<MapReduce<K, V, IV>> (*)();

#endif //SLAVERDD_MAP_REDUCE_H
