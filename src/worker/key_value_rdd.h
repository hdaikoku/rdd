//
// Created by Harunobu Daikoku on 2015/11/20.
//

#ifndef SLAVERDD_KEY_VALUE_RDD_H
#define SLAVERDD_KEY_VALUE_RDD_H

template<typename K, typename V>
class KeyValuesRDD;

#include <iostream>
#include <unordered_map>

#include <mapper.h>
#include <tbb/tbb.h>

#include "worker/rdd.h"
#include "worker/udf.h"

template<typename K, typename V>
class KeyValueRDD: public RDD {
 public:

  KeyValueRDD() {}

  KeyValueRDD(int num_partitions, int partition_id, K empty_key) : RDD(num_partitions, partition_id) {
    key_values_.set_empty_key(empty_key);
  }

  KeyValueRDD(int num_partitions, int partition_id, const google::dense_hash_map<K, V> &key_values)
      : RDD(num_partitions, partition_id), key_values_(key_values) { }

  template<typename NK, typename NV>
  std::unique_ptr<KeyValuesRDD<NK, NV>> Map(const std::string &dl_filename) {
    UDF lib_mapper(dl_filename);

    auto mapper_factory = lib_mapper.LoadFunc<CreateMapper<NK, NV, K, V>>("Create");
    if (mapper_factory == nullptr) {
      std::cerr << "dlsym" << std::endl;
      return nullptr;
    }

    auto mapper = mapper_factory();

    google::dense_hash_map<NK, std::vector<NV>> kvs;
    kvs.set_empty_key("");

    for (const auto &kv : key_values_) {
      mapper->Map(kvs, kv.first, kv.second);
    }

    return std::unique_ptr<KeyValuesRDD<NK, NV>>(new KeyValuesRDD<NK, NV>(num_partitions_,
                                                                          partition_id_,
                                                                          kvs));
  }

  // TODO: implement this for lazy evaluation
  virtual void Compute() override { }

  virtual void Pack(std::vector<msgpack::sbuffer> &buffers) const override {}
  virtual void Unpack(const char *buf, size_t len) override { }

  virtual void PutBlocks(BlockManager &block_mgr) override {}
  virtual void GetBlocks(BlockManager &block_mgr) override { }

  virtual void Print() const override {
    for (const auto kvs : key_values_) {
      std::cout << ToString(kvs.first) << ": " << kvs.second << std::endl;
    }
  }

 protected:
  google::dense_hash_map<K, V> key_values_;

};


#endif //SLAVERDD_KEY_VALUE_RDD_H
