//
// Created by Harunobu Daikoku on 2015/12/04.
//

#include "bi_gram_mapper.h"

void BiGramMapper::Map(std::unordered_map<std::string, std::vector<int>, tbb::tbb_hash<std::string>> &kvs,
                       const long long int &key,
                       const std::string &value) {
  size_t cur = 0, p1, p2 = 0;
  p1 = value.find_first_of(' ', cur);
  while (true) {
    p2 = value.find_first_of(' ', p1 + 1);
    if (p2 == value.npos) {
      kvs[value.substr(cur, value.size() - cur)].emplace_back(1);
      break;
    }
    kvs[value.substr(cur, p2 - cur)].emplace_back(1);
    cur = p1 + 1;
    p1 = p2;
  }
}

void BiGramMapper::Map(std::unordered_map<std::string, std::vector<int>> &kvs,
                       const long long int &key,
                       const std::string &value) {
  size_t cur = 0, p1, p2 = 0;
  p1 = value.find_first_of(' ', cur);
  while (true) {
    p2 = value.find_first_of(' ', p1 + 1);
    if (p2 == value.npos) {
      kvs[value.substr(cur, value.size() - cur)].emplace_back(1);
      break;
    }
    kvs[value.substr(cur, p2 - cur)].emplace_back(1);
    cur = p1 + 1;
    p1 = p2;
  }
}

void BiGramMapper::Map(google::dense_hash_map<std::string, std::vector<int>> &kvs,
                       const long long int &key,
                       const std::string &value) {
  size_t cur = 0, p1, p2 = 0;
  p1 = value.find_first_of(' ', cur);
  while (true) {
    p2 = value.find_first_of(' ', p1 + 1);
    if (p2 == value.npos) {
      kvs[value.substr(cur, value.size() - cur)].emplace_back(1);
      break;
    }
    kvs[value.substr(cur, p2 - cur)].emplace_back(1);
    cur = p1 + 1;
    p1 = p2;
  }
}
