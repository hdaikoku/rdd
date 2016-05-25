//
// Created by Harunobu Daikoku on 2016/02/09.
//

#ifndef PROJECT_TEXT_FILE_RDD_H
#define PROJECT_TEXT_FILE_RDD_H

#include <fstream>
#include "worker/key_value_rdd.h"

class TextFileRDD: public KeyValueRDD<int64_t, std::string> {
 public:

  TextFileRDD(int n_partitions, const std::string &filename, const TextFileIndex &index)
      : KeyValueRDD(n_partitions, index.GetPartitionID()), filename_(filename),
        offset_(index.GetOffset()), size_(index.GetSize()) { }

  virtual void Compute() override {
    std::ifstream ifs(filename_);
    std::unique_ptr<char[]> buf(new char[size_ + 1]);

    ifs.seekg(offset_);
    ifs.read(buf.get(), size_);
    buf[size_] = '\0';
    ifs.close();

    char *save_ptr;
    auto offset = offset_;
    auto line = strtok_r(buf.get(), "\n", &save_ptr);
    while (line != nullptr) {
      auto len = std::char_traits<char>::length(line);
      key_values_.emplace(std::make_pair(offset, std::string(line, len)));
      offset += (len + 1);
      line = strtok_r(nullptr, "\n", &save_ptr);
    }
  }

 private:
  std::string filename_;
  int64_t offset_;
  int32_t size_;
};

#endif //PROJECT_TEXT_FILE_RDD_H
