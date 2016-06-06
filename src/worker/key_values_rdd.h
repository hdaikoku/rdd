//
// Created by Harunobu Daikoku on 2015/11/06.
//

#ifndef SLAVERDD_KEY_VALUES_RDD_H
#define SLAVERDD_KEY_VALUES_RDD_H

template<typename K, typename V>
class KeyValueRDD;

#include <iostream>
#include <unordered_map>
#include <vector>

#include <msgpack.hpp>
#include <reducer.h>

#include "worker/rdd.h"

template<typename K, typename V>
class KeyValuesRDD: public RDD {
 public:

  KeyValuesRDD(int num_partitions, int partition_id, const google::dense_hash_map<K, std::vector<V>> &key_values)
      : RDD(num_partitions, partition_id), key_values_(key_values) { }

  bool Combine(const std::string &dl_filename) {
    void *handle = LoadLib(dl_filename);
    if (handle == NULL) {
      std::cerr << "dlopen" << std::endl;
      return false;
    }

    const auto create_reducer
        = reinterpret_cast<CreateReducer<K, V, K, V>>(LoadFunc(handle, "Create"));
    if (create_reducer == nullptr) {
      std::cerr << "dlsym" << std::endl;
      CloseLib(handle);
      return false;
    }

    auto combiner = create_reducer();

    for (const auto &kv : key_values_) {
      auto combined = combiner->Reduce(kv.first, kv.second);
      key_values_[combined.first].clear();
      key_values_[combined.first].push_back(combined.second);
    }

    combiner.reset(nullptr);
    CloseLib(handle);

    return true;
  }

  template<typename NK, typename NV>
  std::unique_ptr<KeyValueRDD<NK, NV>> Reduce(const std::string &dl_filename) {
    void *handle = LoadLib(dl_filename);
    if (handle == NULL) {
      std::cerr << "dlopen" << std::endl;
      return nullptr;
    }

    const auto create_reducer
        = reinterpret_cast<CreateReducer<NK, NV, K, V>>(LoadFunc(handle, "Create"));
    if (create_reducer == nullptr) {
      std::cerr << "dlsym" << std::endl;
      CloseLib(handle);
      return nullptr;
    }

    auto reducer = create_reducer();

    google::dense_hash_map<NK, NV> kvs;
    kvs.set_empty_key("");

    for (const auto &kv : key_values_) {
      kvs.insert(reducer->Reduce(kv.first, kv.second));
    }

    reducer.reset(nullptr);
    CloseLib(handle);

    return std::unique_ptr<KeyValueRDD<NK, NV>>(new KeyValueRDD<NK, NV>(num_partitions_,
                                                                        partition_id_,
                                                                        kvs));
  }

  // TODO: implement this for lazy evaluation
  virtual void Compute() override { }

  virtual void PutBlocks(BlockManager &block_mgr) override {
    auto num_partitions = block_mgr.GetNumBuffers();
    std::vector<msgpack::sbuffer> buffers(num_partitions);
    Pack(buffers);
    for (int i = 0; i < num_partitions; ++i) {
      block_mgr.PutBlock(i, buffers[i].size(), std::unique_ptr<char[]>(buffers[i].release()));
    }
    key_values_.clear();
  }

  virtual void GetBlocks(BlockManager &block_mgr) override {
    int32_t block_len;
    while (true) {
      auto block = block_mgr.GetBlock(partition_id_, block_len);
      if (block_len == -1) {
        break;
      }
      Unpack(block.get(), block_len);
    }
  }

  virtual void Print() override {
    for (const auto kvs : key_values_) {
      std::cout << ToString(kvs.first) << ": ";
      for (const auto v : kvs.second) {
        std::cout << v << " " << std::endl;
      }
    }
  }

 private:
  google::dense_hash_map<K, std::vector<V>> key_values_;

  virtual void Pack(std::vector<msgpack::sbuffer> &buffers) const override {
    auto num_partitions = buffers.size();
    auto hasher = key_values_.hash_function();
    for (const auto &kv : key_values_) {
      auto dest_id = hasher(kv.first) % num_partitions;
      msgpack::pack(&buffers[dest_id], kv);
    }
  }

  virtual void Unpack(const char *buf, size_t len) override {
    size_t offset = 0;
    msgpack::unpacked unpacked;
    std::pair<K, std::vector<V>> received;
    while (offset != len) {
      msgpack::unpack(&unpacked, buf, len, &offset);
      unpacked.get().convert(&received);
      auto &key = received.first;
      auto &values = received.second;
      std::move(values.begin(), values.end(),
                std::back_inserter(key_values_[key]));
      values.erase(values.begin(), values.end());
    }
  }

};

#endif //SLAVERDD_KEY_VALUES_RDD_H