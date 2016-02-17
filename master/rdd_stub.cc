//
// Created by Harunobu Daikoku on 2016/02/09.
//

#include <jubatus/msgpack/rpc/future.h>
#include "rdd_rpc.h"
#include "master/rdd_stub.h"
#include "master/rdd_context.h"

void RDDStub::AddPartition(int owner, int partition_id) {
  partition_ids_[owner].push_back(partition_id);
}

void RDDStub::GetPartitionIDsByOwner(int owner, std::vector<int> &partition_ids) {
  auto ids = partition_ids_[owner];
  partition_ids.insert(partition_ids.begin(), ids.begin(), ids.end());
}

void RDDStub::Print() const {
  std::vector<msgpack::rpc::future> fs;

  for (auto p : partition_ids_) {
    fs.push_back(rc_.Call("print", p.first, rdd_id_));
  }

  for (auto f : fs) {
    if (f.get<rdd_rpc::Response>() != rdd_rpc::Response::OK) {
      std::cout << "oops" << std::endl;
    }
  }
}
