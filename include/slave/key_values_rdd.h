//
// Created by Harunobu Daikoku on 2015/11/06.
//

#ifndef SLAVERDD_KEY_VALUES_RDD_H
#define SLAVERDD_KEY_VALUES_RDD_H

template<typename K, typename V>
class KeyValueRDD;

#include <unordered_map>
#include <vector>
#include <iostream>
#include <msgpack.hpp>
#include <reducer.h>
#include <dlfcn.h>
#include <socket/socket_server.h>
#include <socket/socket_client.h>
#include "rdd.h"

template<typename K, typename V>
class KeyValuesRDD: public RDD {
 public:

  KeyValuesRDD(const std::unordered_map<K, std::vector<V>, tbb::tbb_hash<K>> &key_values) : key_values_(key_values) { }

  KeyValuesRDD(const tbb::concurrent_unordered_map<K, tbb::concurrent_vector<V>> &key_values) {
    for (const auto &kv : key_values) {
      std::copy(kv.second.begin(), kv.second.end(), std::back_inserter(key_values_[kv.first]));
    }
  }

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
      dlclose(handle);
      return false;
    }

    auto combiner = create_reducer();

    for (const auto &kv : key_values_) {
      auto combined = combiner->Reduce(kv.first, kv.second);
      key_values_[combined.first].clear();
      key_values_[combined.first].push_back(combined.second);
    }

    combiner.release();
    dlclose(handle);

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
      dlclose(handle);
      return nullptr;
    }

    auto reducer = create_reducer();

    tbb::concurrent_unordered_map<NK, NV, tbb::tbb_hash<NK>> kvs;

    tbb::parallel_for_each(key_values_, [&kvs, &reducer](const std::pair<K, std::vector<V>> &kv){
      kvs.insert(reducer->Reduce(kv.first, kv.second));
    });

    reducer.release();
    dlclose(handle);

    return std::unique_ptr<KeyValueRDD<NK, NV>>(new KeyValueRDD<NK, NV>(kvs));
  }

  virtual void PutBlocks(BlockManager &block_mgr) override {
    int n_reducers = block_mgr.GetNumOfBuffers();
    std::vector<msgpack::sbuffer> buffers(n_reducers);
    Pack(buffers);
    for (int i = 0; i < n_reducers; ++i) {
      block_mgr.PutBlock(i, buffers[i].size(), std::unique_ptr<char[]>(buffers[i].release()));
    }
    key_values_.clear();
  }

  virtual void GetBlocks(BlockManager &block_mgr, int my_rank) override {
    long block_len;
    while (true) {
      auto block = block_mgr.GetBlock(my_rank, block_len);
      if (block_len == -1) {
        break;
      }
      Unpack(block_len, block.get());
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
  std::unordered_map<K, std::vector<V>, tbb::tbb_hash<K>> key_values_;

  virtual void Pack(std::vector<msgpack::sbuffer> &buffers) const override {
    auto n_reducers = buffers.size();
    auto hasher = key_values_.hash_function();
    for (const auto &kv : key_values_) {
      auto dest_id = hasher(kv.first) % n_reducers;
      msgpack::pack(&buffers[dest_id], kv);
    }
  }

  virtual void Unpack(long len, const char *buf) override {
    msgpack::unpacker upc;
    upc.reserve_buffer(len);
    memcpy(upc.buffer(), buf, len);
    upc.buffer_consumed(len);

    msgpack::unpacked result;
    while (upc.next(&result)) {
      std::pair<K, std::vector<V>> received;
      result.get().convert(&received);
      std::copy(received.second.begin(), received.second.end(),
                std::back_inserter(key_values_[received.first]));
    }
  }

};

#endif //SLAVERDD_KEY_VALUES_RDD_H