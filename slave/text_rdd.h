//
// Created by Harunobu Daikoku on 2015/11/06.
//

#ifndef SLAVERDD_TEXT_RDD_H
#define SLAVERDD_TEXT_RDD_H

#include <dlfcn.h>
#include <iostream>
#include <unordered_map>
#include <sstream>
#include <map_reduce.h>
#include <memory>
#include "rdd.h"
#include "key_values_rdd.h"


class TextRdd: public Rdd {
 public:
  TextRdd(const std::string &text) : text_(text) { }

  template<typename K, typename V>
  std::unique_ptr<KeyValuesRdd<K, V>> Map(const std::string &dl_filename) {
    void *handle = LoadLib(dl_filename);
    if (handle == NULL) {
      std::cerr << "dlopen" << std::endl;
      return nullptr;
    }

    const auto create_map_reduce
        = reinterpret_cast<CreateMapReduce<K, V, std::string>>(LoadFunc(handle, "Create"));
    if (create_map_reduce == nullptr) {
      std::cerr << "dlsym" << std::endl;
      dlclose(handle);
      return nullptr;
    }

    const auto &map_reduce = create_map_reduce();

    std::unordered_map<std::string, std::vector<int>> kvs;
    std::istringstream str_stream(text_);
    std::string line;

    while (getline(str_stream, line)) {
      map_reduce->Map(kvs, line);
    }

    dlclose(handle);
    return std::unique_ptr<KeyValuesRdd<K, V>>(new KeyValuesRdd<K, V>(kvs));
  }

 private:
  std::string text_;

};


#endif //SLAVERDD_TEXT_RDD_H
