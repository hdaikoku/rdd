//
// Created by Harunobu Daikoku on 2015/11/05.
//

#include <rdd_rpc.h>
#include <jubatus/msgpack/rpc/future.h>
#include "master/rdd_context.h"

std::unique_ptr<KeyValueRDDStub> KeyValuesRDDStub::Reduce(const std::string &dl_filename) {
  std::vector<msgpack::rpc::future> fs;
  int new_rdd_id = rc_.GetNewRddId();

  for (auto p : partition_ids_) {
    rc_.SetTimeout(p.first, 600);
    fs.push_back(rc_.Call("reduce", p.first, rdd_id_, dl_filename, new_rdd_id));
  }

  for (auto f : fs) {
    if (f.get<rdd_rpc::Response>() != rdd_rpc::Response::OK) {
      return nullptr;
    }
  }

  return std::unique_ptr<KeyValueRDDStub>(new KeyValueRDDStub(rc_, new_rdd_id, partition_ids_));
}

// shuffles key-values by pairwise algorithm
bool KeyValuesRDDStub::Shuffle() {
  bool ret = true;
  std::vector<msgpack::rpc::future> fs;

  int n_steps = partition_ids_.size();

  for (int step = 1; step < n_steps; step++) {
    for (auto p : partition_ids_) {
      int dest = p.first ^step;
      std::vector<int> partition_ids;
      GetPartitionIDsByOwner(dest, partition_ids);
      if (dest > p.first) {
        fs.push_back(rc_.Call("shuffle_srv", p.first, partition_ids));
      } else {
        fs.push_back(rc_.Call("shuffle_cli",
                              p.first,
                              partition_ids,
                              rc_.GetSlaveAddrById(dest),
                              rc_.GetSlavePortById(dest)));
      }
    }

    for (auto f : fs) {
      if (f.get<rdd_rpc::Response>() != rdd_rpc::Response::OK) {
        ret = false;
        std::cerr << "oops" << std::endl;
      }
    }

    fs.clear();
    if (!ret) {
      break;
    }

    std::cout << "step " << step << ": OK" << std::endl;
  }

  return ret;
}
