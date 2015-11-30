//
// Created by Harunobu Daikoku on 2015/11/20.
//

#include <sstream>
#include <unordered_map>
#include "word_count_mapper.h"

void WordCountMapper::Map(std::unordered_map<std::string, std::vector<int>> &kvs,
                          const int &key,
                          const std::string &value) {
  size_t cur = 0, pos = 0;
  while ((pos = value.find_first_of(" ", cur)) != std::string::npos) {
    kvs[std::string(value, cur, pos - cur)].push_back(1);
    cur = pos + 1;
  }
}
