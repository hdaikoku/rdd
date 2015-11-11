//
// Created by Harunobu Daikoku on 2015/10/29.
//

#ifndef SLAVERDD_EXECUTOR_H
#define SLAVERDD_EXECUTOR_H

#include <jubatus/msgpack/rpc/server.h>
#include "rdd.h"

class Executor: public msgpack::rpc::dispatcher {

 public:

  Executor(const std::string &addr, int job_port, int data_port)
      : addr_(addr), job_port_(job_port), data_port_(data_port) { }

  virtual void dispatch(msgpack::rpc::request req) override;

  void SetExecutorId(int id);

 private:
  std::string addr_;
  int job_port_;
  int data_port_;
  int id_;
  std::unordered_map<int, std::vector<std::unique_ptr<Rdd>>> rdds_;

  void CreateTextRdd(const int rdd_id, const std::string &data);

};


#endif //SLAVERDD_EXECUTOR_H
