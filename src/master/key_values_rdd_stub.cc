//
// Created by Harunobu Daikoku on 2015/11/05.
//

#include <rdd_context.h>

#include "rdd_rpc.h"

void KeyValuesRDDStub::GroupBy() {
  std::vector<msgpack::rpc::future> fs;

  for (const auto &p : partitions_by_owner_) {
    rc_.SetTimeout(p.first, 600);
    fs.push_back(rc_.Call("group_by", p.first, rdd_id_));
  }

  for (auto &f : fs) {
    if (f.get<rdd_rpc::Response>() != rdd_rpc::Response::OK) {
      return;
    }
  }
}

std::unique_ptr<KeyValueRDDStub> KeyValuesRDDStub::Reduce(const std::string &dl_filename) {
  std::vector<msgpack::rpc::future> fs;
  int new_rdd_id = rc_.GetNewRddId();

  for (const auto &p : partitions_by_owner_) {
    rc_.SetTimeout(p.first, 600);
    fs.push_back(rc_.Call("reduce", p.first, rdd_id_, dl_filename, new_rdd_id));
  }

  for (auto &f : fs) {
    if (f.get<rdd_rpc::Response>() != rdd_rpc::Response::OK) {
      return nullptr;
    }
  }

  return std::unique_ptr<KeyValueRDDStub>(new KeyValueRDDStub(rc_, new_rdd_id, partitions_by_owner_));
}

// shuffles key-values by pairwise algorithm
bool KeyValuesRDDStub::Shuffle() {
  bool ret = true;
  std::vector<msgpack::rpc::future> fs;

  int num_steps = partitions_by_owner_.size();

  for (int step = 1; step < num_steps; step++) {
    for (const auto &p : partitions_by_owner_) {
      int dest = p.first ^step;
      std::vector<int> partition_ids;
      GetPartitionIDsByOwner(dest, partition_ids);
      if (dest > p.first) {
        fs.push_back(rc_.Call("shuffle_srv", p.first, partition_ids));
      } else {
        fs.push_back(rc_.Call("shuffle_cli",
                              p.first,
                              partition_ids,
                              rc_.GetExecutorAddrById(dest),
                              rc_.GetExecutorPortById(dest)));
      }
    }

    for (auto &f : fs) {
      if (f.get<rdd_rpc::Response>() != rdd_rpc::Response::OK) {
        ret = false;
        std::cerr << "oops" << std::endl;
      }
    }

    fs.clear();
    if (!ret) {
      break;
    }
  }

  return ret;
}
