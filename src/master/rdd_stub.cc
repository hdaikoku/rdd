//
// Created by Harunobu Daikoku on 2016/02/09.
//

#include <rdd_context.h>

#include "rdd_rpc.h"

RDDStub::~RDDStub() {
  std::vector<msgpack::rpc::future> fs;

  for (const auto &p : partitions_by_owner_) {
    fs.push_back(rc_.Call("clear", p.first, rdd_id_));
  }

  for (auto &f : fs) {
    if (f.get<rdd_rpc::Response>() != rdd_rpc::Response::OK) {
      std::cout << "oops" << std::endl;
    }
  }
}

void RDDStub::AddPartition(int owner, int partition_id) {
  partitions_by_owner_[owner].push_back(partition_id);
}

void RDDStub::GetOwners(std::vector<int> &owners) const {
  for (const auto &p : partitions_by_owner_) {
    owners.push_back(p.first);
  }
}

void RDDStub::StartShuffleService() const {
  std::vector<msgpack::rpc::future> fs;
  std::string shuffle_type("fully-connected");
  int dest = 0;

  for (const auto &p : partitions_by_owner_) {
    fs.push_back(rc_.Call("shuffle_srv", p.first, shuffle_type, rdd_id_, dest));
    fs.push_back(rc_.Call("shuffle_cli", p.first, shuffle_type, rdd_id_, dest));
  }

  for (auto &f : fs) {
    if (f.get<rdd_rpc::Response>() != rdd_rpc::Response::OK) {
      std::cerr << "oops" << std::endl;
    }
  }
}

void RDDStub::StopShuffleService() const {
  std::vector<msgpack::rpc::future> fs;

  for (const auto &p : partitions_by_owner_) {
    rc_.SetTimeout(p.first, 600);
    fs.push_back(rc_.Call("stop_shuffle", p.first));
  }

  for (auto &f : fs) {
    if (f.get<rdd_rpc::Response>() != rdd_rpc::Response::OK) {
      std::cerr << "oops" << std::endl;
    }
  }
}

void RDDStub::Print() const {
  std::vector<msgpack::rpc::future> fs;

  for (const auto &p : partitions_by_owner_) {
    fs.push_back(rc_.Call("print", p.first, rdd_id_));
  }

  for (auto &f : fs) {
    if (f.get<rdd_rpc::Response>() != rdd_rpc::Response::OK) {
      std::cerr << "oops" << std::endl;
    }
  }
}
