//
// Created by Harunobu Daikoku on 2015/11/06.
//

#ifndef SLAVERDD_KEY_VALUES_RDD_H
#define SLAVERDD_KEY_VALUES_RDD_H

#include <unordered_map>
#include <vector>
#include <iostream>
#include <msgpack.hpp>
#include "rdd.h"

template<typename K, typename V>
class KeyValuesRdd: public Rdd {
 public:
  KeyValuesRdd(const std::unordered_map<K, std::vector<V>> &key_values) : key_values_(key_values) { }

  std::unique_ptr<KeyValuesRdd<K, V>> Reduce(const std::string &dl_filename);

  // TODO temporary hack for specifying data_port
  bool ShuffleServer(int dest_id, int n_reducers, int port);
  //  bool ShuffleServer(int dest_id, int n_reducers);

  bool ShuffleClient(const std::string &dst_addr, int dest_id, int n_reducers);

  void PrintPairs() {
    for (const auto kvs : key_values_) {
      std::cout << kvs.first << ": ";
      for (const auto v : kvs.second) {
        std::cout << v << " " << std::endl;
      }
    }
  }

 private:
  std::unordered_map<K, std::vector<V>> key_values_;

  void PackKeyValuesFor(int dest, int n_reducers, msgpack::sbuffer &sbuf);
  void UnpackKeyValues(const char *buf, size_t len);

};

#include "key_values_rdd.cc"

#endif //SLAVERDD_KEY_VALUES_RDD_H