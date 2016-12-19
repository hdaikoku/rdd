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

    for (const auto &p : partitions_by_owner_) {
      fs.push_back(rc_.Call("pairwise_shuffle", p.first, rdd_id_));
    }

    for (auto &f : fs) {
      if (f.get<rdd_rpc::Response>() != rdd_rpc::Response::OK) {
        std::cerr << "oops" << std::endl;
      }
    }
  } else if (shuffle_type == "fully-connected") {
    StartShuffleService();
    StopShuffleService();
  }

  return true;
}
