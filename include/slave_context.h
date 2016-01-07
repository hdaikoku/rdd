//
// Created by Harunobu Daikoku on 2016/01/07.
//

#ifndef PROJECT_SLAVE_CONTEXT_H
#define PROJECT_SLAVE_CONTEXT_H

#include <string>
#include <msgpack.hpp>

class SlaveContext {
 public:

  SlaveContext() { }
  SlaveContext(const std::string &addr, int job_port, int data_port)
      : addr_(addr), job_port_(job_port), data_port_(data_port) { }

  std::string GetAddr() const {
    return addr_;
  }

  int GetJobPort() const {
    return job_port_;
  }

  int GetDataPort() const {
    return data_port_;
  }

  MSGPACK_DEFINE(addr_, job_port_, data_port_);

 private:
  std::string addr_;
  int job_port_;
  int data_port_;
};

#endif //PROJECT_SLAVE_CONTEXT_H
