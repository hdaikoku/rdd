//
// Created by Harunobu Daikoku on 2015/11/07.
//

#ifndef SLAVERDD_WORD_COUNT_MAP_REDUCE_H
#define SLAVERDD_WORD_COUNT_MAP_REDUCE_H

#include <string>
#include <map_reduce.h>

class WordCountMapReduce: public MapReduce<std::string, int, std::string> {
 public:
  virtual void Map(std::unordered_map<std::string, std::vector<int>> &kvs, const std::string &value) override;
  virtual std::pair<std::string, int> Reduce(const std::string &key, const std::vector<int> &values) override;
};

extern "C" std::unique_ptr<MapReduce<std::string, int, std::string>> Create() {
  return std::unique_ptr<MapReduce<std::string, int, std::string>>(new WordCountMapReduce);
}

#endif //SLAVERDD_WORD_COUNT_MAP_REDUCE_H
