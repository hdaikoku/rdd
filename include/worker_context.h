//
// Created by Harunobu Daikoku on 2016/01/07.
//

#ifndef PROJECT_SLAVE_CONTEXT_H
#define PROJECT_SLAVE_CONTEXT_H

#include <string>
#include <msgpack.hpp>

class WorkerContext {
 public:

  WorkerContext() { }
  WorkerContext(const std::string &addr, const std::string &job_port, const std::string &data_port)
      : addr_(addr), job_port_(job_port), data_port_(data_port) { }

  std::string GetAddr() const {
    return addr_;
  }

  std::string GetJobPort() const {
    return job_port_;
  }

  std::string GetDataPort() const {
    return data_port_;
  }

  MSGPACK_DEFINE(addr_, job_port_, data_port_);

 private:
  std::string addr_;
  std::string job_port_;
  std::string data_port_;
};

#endif //PROJECT_SLAVE_CONTEXT_H
