//
// Created by Harunobu Daikoku on 2015/11/20.
//

#ifndef SLAVERDD_WORD_COUNT_MAPPER_H
#define SLAVERDD_WORD_COUNT_MAPPER_H

#include <mapper.h>
#include <string>
#include <unordered_map>

class WordCountMapper: public Mapper<std::string, int, long long int, std::string> {
 public:

  virtual void Map(std::unordered_map<std::string, std::vector<int>, tbb::tbb_hash<std::string>> &kvs,
                   const long long int &key, const std::string &value) override;

  virtual void
      Map(std::unordered_map<std::string, std::vector<int>> &kvs, const long long int &key, const std::string &value)
      override;

  virtual void Map
      (google::dense_hash_map<std::string, std::vector<int>> &kvs, const long long int &key, const std::string &value)
      override;

};

extern "C" std::unique_ptr<Mapper<std::string, int, long long int, std::string>> Create() {
  return std::unique_ptr<Mapper<std::string, int, long long int, std::string>>(new WordCountMapper);
}


#endif //SLAVERDD_WORD_COUNT_MAPPER_H
