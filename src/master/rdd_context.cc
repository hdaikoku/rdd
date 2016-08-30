//
// Created by Harunobu Daikoku on 2015/10/29.
//

#include <fstream>
#include <jubatus/msgpack/rpc/client.h>
#include <rdd_context.h>

#include "rdd_rpc.h"

void RDDContext::Init() {
  last_rdd_id_ = 0;
  auto num_executors = executors_.size();

  sp_.set_pool_size_limit(num_executors);

  std::vector<msgpack::rpc::future> fs;
  int executor_id;
  for (executor_id = 0; executor_id < num_executors; executor_id++) {
    fs.push_back(Call("hello", executor_id, executor_id, executors_));
  }

  for (executor_id = 0; executor_id < num_executors; executor_id++) {
    if (fs[executor_id].get<rdd_rpc::Response>() != rdd_rpc::Response::OK) {
      std::cerr << "could not connect to "
          << executors_[executor_id].GetAddr() << ":" << executors_[executor_id].GetJobPort() << std::endl;
    }
  }
}

// opens the file, makes indices, sends them to executors and returns a stub for TextFileRDD
std::unique_ptr<TextFileRDDStub> RDDContext::TextFile(const std::string &filename) {
  return TextFileRDDStub::NewInstance(*this, filename);
}

// Returns number of slaves
int RDDContext::GetNumExecutors() const {
  return executors_.size();
}

// Returns new RDD id
int RDDContext::GetNewRddId() {
  return last_rdd_id_++;
}

std::string RDDContext::GetExecutorAddrById(const int &id) const {
  return executors_[id].GetAddr();
}

std::string RDDContext::GetExecutorPortById(const int &id) const {
  return executors_[id].GetDataPort();
}

void RDDContext::SetTimeout(int dest, unsigned int timeout) {
  sp_.get_session(executors_[dest].GetAddr(), std::stoi(executors_[dest].GetJobPort())).set_timeout(timeout);
}