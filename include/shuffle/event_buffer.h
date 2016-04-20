//
// Created by Harunobu Daikoku on 2016/02/25.
//

#ifndef FULLY_CONNECTED_BUFFER_H
#define FULLY_CONNECTED_BUFFER_H

#include <stddef.h>
#include <memory>

class EventBuffer {
 public:
  EventBuffer() : limit_(0), size_(0) { }
  EventBuffer(size_t buf_size) : limit_(buf_size), buf_(new char[buf_size]), size_(0) { }

  operator bool() const {
    return (limit_ != 0);
  }

  void Init(size_t buf_size) {
    limit_ = buf_size;
    size_ = 0;
    buf_.reset(new char[buf_size]);
  }

  void Clear() {
    size_ = 0;
  }

  char *Get() const {
    return (char *) buf_.get();
  }

  size_t GetLimit() const {
    return limit_;
  }

  size_t GetSize() const {
    return size_;
  }

  void Consumed(size_t consumed) {
    size_ += consumed;
  }

  template<typename T>
  T &As() const {
    return *reinterpret_cast<T *>(buf_.get());
  }

 private:
  std::unique_ptr<char[]> buf_;
  size_t limit_;
  size_t size_;
};
#endif //FULLY_CONNECTED_BUFFER_H

