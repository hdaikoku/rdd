//
// Created by Harunobu Daikoku on 2016/03/11.
//

#ifndef PROJECT_GROUP_BY_H
#define PROJECT_GROUP_BY_H

#include <mapper.h>

class GroupByMapper: public Mapper<int, std::string, long long int, std::string> {

 public:
  virtual void Map(
      google::dense_hash_map<int, std::vector<std::string>> &kvs,
      const long long int &key,
      const std::string &value) override;
};

extern "C" std::unique_ptr<Mapper<int, std::string, long long int, std::string>> Create() {
  return std::unique_ptr<Mapper<int, std::string, long long int, std::string>>(new GroupByMapper);
}


#endif //PROJECT_GROUP_BY_H
