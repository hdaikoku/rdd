//
// Created by Harunobu Daikoku on 2016/02/09.
//

#ifndef PROJECT_TEXT_FILE_RDD_H
#define PROJECT_TEXT_FILE_RDD_H

#include <fstream>
#include "key_value_rdd.h"

class TextFileRDD: public KeyValueRDD<uint64_t, std::string> {
 public:

  TextFileRDD(const std::string &file_name, uint64_t chunk_offset, uint32_t chunk_size)
      : KeyValueRDD(), file_name_(file_name), chunk_offset_(chunk_offset), chunk_size_(chunk_size) { }

  virtual void Compute() override {
    std::ifstream ifs(file_name_);
    std::unique_ptr<char[]> buf(new char[chunk_size_ + 1]);

    ifs.seekg(chunk_offset_);
    ifs.read(buf.get(), chunk_size_);
    buf[chunk_size_] = '\0';
    ifs.close();

    char *save_ptr;
    auto offset = chunk_offset_;
    auto line = strtok_r(buf.get(), "\n", &save_ptr);
    while (line != nullptr) {
      auto len = std::char_traits<char>::length(line);
      key_values_.emplace(std::make_pair(offset, std::string(line, len)));
      offset += (len + 1);
      line = strtok_r(nullptr, "\n", &save_ptr);
    }
  }

 private:
  std::string file_name_;
  uint64_t chunk_offset_;
  uint32_t chunk_size_;
};

#endif //PROJECT_TEXT_FILE_RDD_H
