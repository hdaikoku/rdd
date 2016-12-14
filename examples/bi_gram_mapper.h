//
// Created by Harunobu Daikoku on 2015/12/04.
//

#ifndef PROJECT_BI_GRAM_MAPPER_H
#define PROJECT_BI_GRAM_MAPPER_H

#include <mapper.h>

class BiGramMapper:
    public Mapper<std::string, int, int64_t, std::string> {

 public:

  virtual void Map(
      google::dense_hash_map<std::string, std::vector<int>> &kvs,
      const int64_t &key,
      const std::string &value) override;

};

extern "C" std::unique_ptr<Mapper<std::string, int, int64_t, std::string>> Create() {
  return std::unique_ptr<Mapper<std::string, int, int64_t, std::string>>(new BiGramMapper);
}


#endif //PROJECT_BI_GRAM_MAPPER_H
