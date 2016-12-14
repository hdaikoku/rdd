//
// Created by Harunobu Daikoku on 2016/03/11.
//

#ifndef PROJECT_GROUP_BY_H
#define PROJECT_GROUP_BY_H

#include <mapper.h>

class GroupByMapper: public Mapper<int, std::string, int64_t, std::string> {

 public:
  virtual void Map(
      google::dense_hash_map<int, std::vector<std::string>> &kvs,
      const int64_t &key,
      const std::string &value) const override;
};

extern "C" std::unique_ptr<Mapper<int, std::string, int64_t, std::string>> Create() {
  return std::unique_ptr<Mapper<int, std::string, int64_t, std::string>>(new GroupByMapper);
}


#endif //PROJECT_GROUP_BY_H
