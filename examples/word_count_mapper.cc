//
// Created by Harunobu Daikoku on 2015/11/20.
//

#include <sstream>
#include <unordered_map>
#include "word_count_mapper.h"

void WordCountMapper::Map(google::dense_hash_map<std::string, std::vector<int>> &kvs,
                          const long long int &key,
                          const std::string &value) {
  size_t cur = 0, pos;
  while ((pos = value.find_first_of(' ', cur)) != value.npos) {
    kvs[value.substr(cur, pos - cur)].emplace_back(1);
    cur = pos + 1;
  }
  kvs[value.substr(cur, value.size() - cur)].emplace_back(1);
}
