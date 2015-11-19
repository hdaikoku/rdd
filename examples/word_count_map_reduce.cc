//
// Created by Harunobu Daikoku on 2015/11/07.
//

#include <numeric>
#include <sstream>
#include <iostream>
#include "word_count_map_reduce.h"

void WordCountMapReduce::Map(std::unordered_map<std::string, std::vector<int>> &kvs, const std::string &value) {
  std::string word;
  std::istringstream iss(value);

  while (iss >> word) {
    if (kvs.find(word) == kvs.end()) {
      kvs[word].push_back(0);
    }
    kvs[word][0] += 1;
  }

}

std::pair<std::string, int> WordCountMapReduce::Reduce(const std::string &key, const std::vector<int> &values) {
  return std::make_pair(key, std::accumulate(values.begin(), values.end(), 0));
}
