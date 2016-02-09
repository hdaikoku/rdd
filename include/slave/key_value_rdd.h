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

  KeyValueRDD(tbb::concurrent_unordered_map<K, V> &&key_values) {
    for (auto &&kv : key_values) {
      key_values_.insert(kv);
    }
  }

  template<typename NK, typename NV>
  std::unique_ptr<KeyValuesRDD<NK, NV>> Map(const std::string &dl_filename) {
    if (key_values_.size() == 0) {
      Compute();
    }

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

    google::dense_hash_map<NK, std::vector<NV>> kvs;
    kvs.set_empty_key("");

    for (const auto &kv : key_values_) {
      mapper->Map(kvs, kv.first, kv.second);
    }

    mapper.reset(nullptr);
    dlclose(handle);

    return std::unique_ptr<KeyValuesRDD<NK, NV>>(new KeyValuesRDD<NK, NV>(std::move(kvs)));
  }

  // TODO: implement this for lazy evaluation
  virtual void Compute() override { }

  virtual void Pack(std::vector<msgpack::sbuffer> &buffers) const override {}
  virtual void Unpack(const char *buf, size_t len) override { }

  virtual void PutBlocks(BlockManager &block_mgr) override {}
  virtual void GetBlocks(BlockManager &block_mgr, int my_rank) override {}

  virtual void Print() override {
    for (const auto kvs : key_values_) {
      std::cout << ToString(kvs.first) << ": " << kvs.second << std::endl;
    }
  }

 protected:
  std::unordered_map<K, V, tbb::tbb_hash<K>> key_values_;

};


#endif //SLAVERDD_KEY_VALUE_RDD_H
