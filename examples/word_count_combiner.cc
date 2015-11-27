//
// Created by Harunobu Daikoku on 2015/11/27.
//

#include <numeric>
#include "word_count_combiner.h"

std::pair<std::string, int> WordCountCombiner::Reduce(const std::string &key, const std::vector<int> &values) {
  return std::make_pair(key, std::accumulate(values.begin(), values.end(), 0));
}