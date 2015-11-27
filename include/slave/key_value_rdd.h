//
// Created by Harunobu Daikoku on 2015/11/20.
//

#ifndef SLAVERDD_KEY_VALUE_RDD_H
#define SLAVERDD_KEY_VALUE_RDD_H

template<typename K, typename V>
class KeyValuesRDD;

#include <iostream>
#include <tbb/tbb.h>
#include <mapper.h>
#include <dlfcn.h>
#include <unordered_map>
#include "rdd.h"

template<typename K, typename V>
class KeyValueRDD: public RDD {
 public:

  KeyValueRDD() {}

  KeyValueRDD(const std::unordered_map<K, V> &key_values) : key_values_(key_values) { }

  void Insert(const K &key, const V &value) {
    key_values_.insert(std::make_pair(key, value));
  }

  template<typename NK, typename NV>
  std::unique_ptr<KeyValuesRDD<NK, NV>> Map(const std::string &dl_filename) {
    void *handle = LoadLib(dl_filename);
    if (handle == NULL) {
      std::cerr << "dlopen" << std::endl;
      return nullptr;
    }

    const auto create_mapper
        = reinterpret_cast<CreateMapper<NK, NV, K, V>>(LoadFunc(handle, "Create"));
    if (create_mapper == nullptr) {
      std::cerr << "dlsym" << std::endl;
      dlclose(handle);
      return nullptr;
    }

    auto mapper = create_mapper();

    tbb::concurrent_unordered_map<NK, tbb::concurrent_vector<NV>> kvs;
    tbb::parallel_for_each(key_values_, [&kvs, &mapper](const std::pair<K, V> &kv) {
      mapper->Map(kvs, kv.first, kv.second);
    });

    mapper.release();
    dlclose(handle);

    return std::unique_ptr<KeyValuesRDD<NK, NV>>(new KeyValuesRDD<NK, NV>(kvs));
  }

  virtual void Print() override {
    for (const auto kvs : key_values_) {
      std::cout << kvs.first << ": " << kvs.second << std::endl;
    }
  }

 private:
  std::unordered_map<K, V> key_values_;

};


#endif //SLAVERDD_KEY_VALUE_RDD_H
