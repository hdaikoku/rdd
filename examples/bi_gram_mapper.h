//
// Created by Harunobu Daikoku on 2015/12/04.
//

#ifndef PROJECT_BI_GRAM_MAPPER_H
#define PROJECT_BI_GRAM_MAPPER_H

#include <mapper.h>

class BiGramMapper:
    public Mapper<std::pair<std::string, std::string>, int, long long int, std::string> {

 public:

  virtual void Map(std::unordered_map<std::pair<std::string, std::string>,
                                      std::vector<int>,
                                      tbb::tbb_hash<std::pair<std::string, std::string>>> &kvs,
                   const long long int &key,
                   const std::string &value) override;

};

extern "C" std::unique_ptr<Mapper<std::pair<std::string, std::string>, int, long long int, std::string>> Create() {
  return std::unique_ptr<Mapper<std::pair<std::string, std::string>,
                                int,
                                long long int,
                                std::string>>(new BiGramMapper);
}


#endif //PROJECT_BI_GRAM_MAPPER_H
