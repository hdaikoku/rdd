//
// Created by Harunobu Daikoku on 2016/02/17.
//

#ifndef PROJECT_TEXT_FILE_INDEX_H
#define PROJECT_TEXT_FILE_INDEX_H

#include <msgpack.hpp>

class TextFileIndex {
 public:

  TextFileIndex() { }

  TextFileIndex(int partition_id, int64_t offset, int32_t size)
      : partition_id_(partition_id), offset_(offset), size_(size) { }


  int GetPartitionID() const {
    return partition_id_;
  }

  int64_t GetOffset() const {
    return offset_;
  }

  int32_t GetSize() const {
    return size_;
  }

  MSGPACK_DEFINE(partition_id_, offset_, size_);

 private:
  int partition_id_;
  int64_t offset_;
  int32_t size_;
};

#endif //PROJECT_TEXT_FILE_INDEX_H
