//
// Created by Harunobu Daikoku on 2016/03/11.
//

#include "group_by_mapper.h"

void GroupByMapper::Map(google::dense_hash_map<int, std::vector<std::string>> &kvs,
                        const long long int &key,
                        const std::string &value) {
  size_t cur = 0, pos;
  while ((pos = value.find_first_of(' ', cur)) != value.npos) {
    auto word = value.substr(cur, pos - cur);
    kvs[word.length()].push_back(word);
    cur = pos + 1;
  }
  auto word = value.substr(cur, value.length() - cur);
  kvs[word.length()].push_back(word);
}
