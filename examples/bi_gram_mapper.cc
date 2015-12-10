//
// Created by Harunobu Daikoku on 2015/12/04.
//

#include "bi_gram_mapper.h"

void BiGramMapper::Map(std::unordered_map<std::pair<std::string, std::string>,
                                          std::vector<int>,
                                          Hasher<std::pair<std::string, std::string>>> &kvs,
                       const long long int &key,
                       const std::string &value) {
  size_t cur = 0, pos = 0;
  std::string word1, word2;

  if ((pos = value.find_first_of(" ", cur)) == std::string::npos) {
    return;
  }

  word1 = std::string(value, cur, pos - cur);
  cur = pos + 1;

  while ((pos = value.find_first_of(" ", cur)) != std::string::npos) {
    word2 = std::string(value, cur, pos - cur);
    kvs[std::make_pair(word1, word2)].push_back(1);
    cur = pos + 1;
    word1 = word2;
  }
}
