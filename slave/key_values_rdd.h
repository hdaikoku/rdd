//
// Created by Harunobu Daikoku on 2015/11/06.
//

#ifndef SLAVERDD_KEY_VALUES_RDD_H
#define SLAVERDD_KEY_VALUES_RDD_H

#include <unordered_map>
#include <vector>
#include <iostream>
#include "rdd.h"

template<typename K, typename V>
class KeyValuesRdd: public Rdd {
 public:
  KeyValuesRdd(const std::unordered_map<K, std::vector<V>> &key_values) : key_values_(key_values) { }

  void PrintPairs() {
    for (const auto kvs : key_values_) {
      for (const auto v : kvs.second) {
        std::cout << kvs.first << ": " << v << std::endl;
      }
    }
  }

 private:
  std::unordered_map<K, std::vector<V>> key_values_;
};


#endif //SLAVERDD_KEY_VALUES_RDD_H
