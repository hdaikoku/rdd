//
// Created by Harunobu Daikoku on 2016/05/07.
//

#ifndef PROJECT_RECV_BUFFER_H
#define PROJECT_RECV_BUFFER_H

#include <iostream>

class RecvBuffer {
 public:

  RecvBuffer() : size_(0), capacity_(0) { }

  RecvBuffer(int32_t capacity) : buf_(capacity), capacity_(capacity), size_(0) {
    std::cout << "recvbuffer " << capacity << " bytes" << std::endl;
  }

  void Reset(int32_t capacity) {
    size_ = 0;
    capacity_ = capacity;
    if (capacity > buf_.capacity()) {
      buf_.reserve(capacity);
    }
  }

  char *Get() {
    return buf_.data() + size_;
  }

  int32_t GetCapacity() const {
    return capacity_;
  }

  int32_t GetSize() const {
    return capacity_ - size_;
  }

  bool IsFull() const {
    return (capacity_ == size_);
  }

  void Consumed(int32_t consumed) {
    size_ += consumed;
  }

  template<typename T>
  const T &As() const {
    return *reinterpret_cast<const T *>(buf_.data());
  }

  const char *Data() const {
    return buf_.data();
  }

 private:
  std::vector<char> buf_;
  int32_t capacity_;
  int32_t size_;

};

#endif //PROJECT_RECV_BUFFER_H
