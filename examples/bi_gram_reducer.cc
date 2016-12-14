//
// Created by Harunobu Daikoku on 2015/12/09.
//

#include <numeric>
#include "bi_gram_reducer.h"

std::pair<std::string, int> BiGramReducer::Reduce(const std::string &key,
                                                  const std::vector<int> &values) const {
  return std::make_pair(key, std::accumulate(values.begin(), values.end(), 0));
}
