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

  KeyValueRDD(const std::unordered_map<K, V, tbb::tbb_hash<K>> &key_values) : key_values_(key_values) { }

  KeyValueRDD(const tbb::concurrent_unordered_map<K, V, tbb::tbb_hash<K>> &key_values) {
    for (const auto &kv : key_values) {
      key_values_[kv.first] = kv.second;
    }
  }

  KeyValueRDD(const std::string &filename, const long long int offset, const int size)
      : filename_(filename), chunk_offset_(offset), chunk_size_(size) {};

  void Insert(const K &key, const V &value) {
    key_values_.insert(std::make_pair(key, value));
  }

  void Materialize() {
    std::ifstream ifs(filename_);
    std::unique_ptr<char []> buf(new char[chunk_size_ + 1]);

    ifs.seekg(chunk_offset_);
    ifs.read(buf.get(), chunk_size_);
    buf[chunk_size_] = '\0';
    ifs.close();

    size_t cur = 0, pos = 0;
    std::string text(buf.get());
    while ((pos = text.find_first_of("\n", cur)) != std::string::npos) {
      key_values_.insert(std::make_pair(chunk_offset_ + cur, std::string(text, cur, pos - cur)));
      cur = pos + 1;
    }
  }

  template<typename NK, typename NV>
  std::unique_ptr<KeyValuesRDD<NK, NV>> Map(const std::string &dl_filename) {
    if (key_values_.size() == 0) {
      Materialize();
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

    std::unordered_map<NK, std::vector<NV>, tbb::tbb_hash<NK>> kvs;
    for (const auto &kv : key_values_) {
      mapper->Map(kvs, kv.first, kv.second);
    }

    mapper.release();
    dlclose(handle);

    return std::unique_ptr<KeyValuesRDD<NK, NV>>(new KeyValuesRDD<NK, NV>(kvs));
  }

  virtual void Pack(std::vector<msgpack::sbuffer> &buffers) const override {}
  virtual void Unpack(long len, const char *buf) override {}

  virtual void PutBlocks(BlockManager &block_mgr) override {}
  virtual void GetBlocks(BlockManager &block_mgr, int my_rank) override {}

  virtual void Print() override {
    for (const auto kvs : key_values_) {
      std::cout << ToString(kvs.first) << ": " << kvs.second << std::endl;
    }
  }

 private:
  std::unordered_map<K, V, tbb::tbb_hash<K>> key_values_;
  std::string filename_;
  long long int chunk_offset_;
  int chunk_size_;

};


#endif //SLAVERDD_KEY_VALUE_RDD_H
