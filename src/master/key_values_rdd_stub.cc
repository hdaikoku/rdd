//
// Created by Harunobu Daikoku on 2015/11/05.
//

#include <rdd_context.h>

#include "rdd_rpc.h"

void KeyValuesRDDStub::GroupBy() {
  if (!shuffled_) {
    shuffled_ = Shuffle();
  }

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

std::unique_ptr<KeyValueRDDStub> KeyValuesRDDStub::Reduce(const std::string &dl_reducer) {
  if (!shuffled_) {
    shuffled_ = Shuffle();
  }

  std::vector<msgpack::rpc::future> fs;
  int new_rdd_id = rc_.GetNewRddId();
  std::string dl_reducer_path(realpath(dl_reducer.c_str(), NULL));
  
  for (const auto &p : partitions_by_owner_) {
    rc_.SetTimeout(p.first, 600);
    fs.push_back(rc_.Call("reduce", p.first, rdd_id_, dl_reducer_path, new_rdd_id));
  }

  for (auto &f : fs) {
    if (f.get<rdd_rpc::Response>() != rdd_rpc::Response::OK) {
      return nullptr;
    }
  }

  return std::unique_ptr<KeyValueRDDStub>(new KeyValueRDDStub(rc_, new_rdd_id, partitions_by_owner_));
}

bool KeyValuesRDDStub::Shuffle() {
  auto shuffle_type = rc_.GetShuffleType();
  if (shuffle_type == "pairwise") {
    std::vector<msgpack::rpc::future> fs;

    int num_steps = partitions_by_owner_.size();

    std::string shuffle_type("pairwise");

    for (int step = 1; step < num_steps; step++) {
      for (const auto &p : partitions_by_owner_) {
        int dest = p.first ^step;
        if (dest > p.first) {
          fs.push_back(rc_.Call("shuffle_srv", p.first, shuffle_type, rdd_id_, dest));
        } else {
          fs.push_back(rc_.Call("shuffle_cli", p.first, shuffle_type, rdd_id_, dest));
        }
      }

      for (auto &f : fs) {
        if (f.get<rdd_rpc::Response>() != rdd_rpc::Response::OK) {
          std::cerr << "oops" << std::endl;
        }
      }

      fs.clear();
    }
  } else if (shuffle_type == "fully-connected") {
    StartShuffleService();
    StopShuffleService();
  }

  return true;
}
