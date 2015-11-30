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

  KeyValueRDD(const tbb::concurrent_unordered_map<K, V> &key_values) {
    for (const auto &kv : key_values) {
      key_values_[kv.first] = kv.second;
    }
  }

  KeyValueRDD(const std::string &filename, const std::vector<std::pair<long long int, int>> &indices)
      : filename_(filename), indices_(indices) {};

  void Insert(const K &key, const V &value) {
    key_values_.insert(std::make_pair(key, value));
  }

  static void ReadChunk(std::unordered_map<long long int, std::string> &kvs,
                 const std::string &filename, long long int offset, int size) {
    std::ifstream ifs(filename);
    std::unique_ptr<char []> buf(new char[size + 1]);

    ifs.seekg(offset);
    ifs.read(buf.get(), size);
    std::cout << "read: " << size << " bytes" << std::endl;
    buf[size] = '\0';
    ifs.close();

    size_t cur = 0, pos = 0;
    std::string text(buf.get());
    while ((pos = text.find_first_of("\n", cur)) != std::string::npos) {
      kvs.insert(std::make_pair(offset + cur, std::string(text, cur, pos - cur)));
      cur = pos + 1;
    }
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

    std::string filename(filename_);

    tbb::concurrent_unordered_map<NK, tbb::concurrent_vector<NV>> kvs;
    tbb::parallel_for_each(indices_, [&kvs, &mapper, &filename](const std::pair<long long int, int> &index){
      std::unordered_map<K, V> tkvs;
      ReadChunk(tkvs, filename, index.first, index.second);
      std::cout << "tvks size: " << tkvs.size() << std::endl;
      std::unordered_map<NK, std::vector<NV>> key_values;
      for (const auto &kv : tkvs) {
        mapper->Map(key_values, kv.first, kv.second);
      }

      for (const auto &kv : key_values) {
        std::copy(kv.second.begin(), kv.second.end(), std::back_inserter(kvs[kv.first]));
      }
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
  std::vector<std::pair<long long int, int>> indices_;
  std::string filename_;

};


#endif //SLAVERDD_KEY_VALUE_RDD_H
