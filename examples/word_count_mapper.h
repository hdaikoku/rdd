//
// Created by Harunobu Daikoku on 2015/11/20.
//

#ifndef SLAVERDD_WORD_COUNT_MAPPER_H
#define SLAVERDD_WORD_COUNT_MAPPER_H

#include <mapper.h>
#include <string>
#include <unordered_map>

class WordCountMapper: public Mapper<std::string, int, int64_t, std::string> {
 public:

  virtual void Map(
      google::dense_hash_map<std::string, std::vector<int>> &kvs,
      const int64_t &key,
      const std::string &value) const override;

};

extern "C" std::unique_ptr<Mapper<std::string, int, int64_t, std::string>> Create() {
  return std::unique_ptr<Mapper<std::string, int, int64_t, std::string>>(new WordCountMapper);
}


#endif //SLAVERDD_WORD_COUNT_MAPPER_H
