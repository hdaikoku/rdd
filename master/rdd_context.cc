//
// Created by Harunobu Daikoku on 2015/10/29.
//

#include <fstream>
#include <jubatus/msgpack/rpc/client.h>
#include "master/rdd_context.h"
#include "rdd_rpc.h"

void RDDContext::Init() {
  last_rdd_id_ = 0;
  auto n_slaves = slaves_.size();

  sp_.set_pool_size_limit(n_slaves);

  std::vector<msgpack::rpc::future> fs;
  int slave_id;
  for (slave_id = 0; slave_id < n_slaves; slave_id++) {
    fs.push_back(Call("hello", slave_id, slave_id, slaves_));
  }

  for (slave_id = 0; slave_id < n_slaves; slave_id++) {
    if (fs[slave_id].get<rdd_rpc::Response>() != rdd_rpc::Response::OK) {
      std::cerr << "could not connect to "
          << slaves_[slave_id].GetAddr() << ":" << slaves_[slave_id].GetJobPort() << std::endl;
    }
  }
}

// opens the file, makes indices, sends them to slaves and returns a stub for TextFileRDD
std::unique_ptr<TextFileRDDStub> RDDContext::TextFile(const std::string &filename) {
  return TextFileRDDStub::NewInstance(*this, filename);
}

// Returns number of slaves
int RDDContext::GetNumSlaves() const {
  return slaves_.size();
}

// Returns new RDD id
int RDDContext::GetNewRddId() {
  return last_rdd_id_++;
}

std::string RDDContext::GetSlaveAddrById(const int &id) const {
  return slaves_[id].GetAddr();
}

int RDDContext::GetSlavePortById(const int &id) const {
  return slaves_[id].GetDataPort();
}

void RDDContext::SetTimeout(int dest, unsigned int timeout) {
  sp_.get_session(slaves_[dest].GetAddr(), slaves_[dest].GetJobPort()).set_timeout(timeout);
}