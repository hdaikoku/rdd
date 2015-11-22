//
// Created by Harunobu Daikoku on 2015/11/20.
//

#include <sstream>
#include "word_count_mapper.h"

void WordCountMapper::Map(tbb::concurrent_unordered_map<std::string, tbb::concurrent_vector<int>> &kvs,
                          const int &key,
                          const std::string &value) {
  std::string word;
  std::istringstream iss(value);

  while (iss >> word) {
    kvs[word].push_back(1);
  }

}
