//
// Created by Harunobu Daikoku on 2015/12/09.
//

#include <numeric>
#include "bi_gram_reducer.h"

std::pair<std::pair<std::string, std::string>, int> BiGramReducer::Reduce(const std::pair<std::string,
                                                                                          std::string> &key,
                                                                          const std::vector<int> &values) {
  return std::make_pair(key, std::accumulate(values.begin(), values.end(), 0));
}
