//
// Created by Harunobu Daikoku on 2016/04/19.
//

#ifndef PROJECT_SEND_BUFFER_H
#define PROJECT_SEND_BUFFER_H

#include <memory>

class SendBuffer {
 public:

  SendBuffer() : size_(0), consumed_(0) { }

  SendBuffer(std::unique_ptr<char[]> data, int32_t size)
      : data_(std::move(data)), size_(size), consumed_(0) { }

  SendBuffer(void *data, int32_t size) : data_(new char[size]), size_(size), consumed_(0) {
    std::memcpy(data_.get(), data, size);
  }

  void *Get() const {
    return data_.get() + consumed_;
  }

  int32_t GetSize() const {
    return size_ - consumed_;
  }

  void Consumed(int32_t consumed) {
    consumed_ += consumed;
  }

 private:
  std::unique_ptr<char[]> data_;
  int32_t size_;
  int32_t consumed_;
};

#endif //PROJECT_SEND_BUFFER_H
