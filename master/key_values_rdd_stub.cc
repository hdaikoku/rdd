//
// Created by Harunobu Daikoku on 2015/11/05.
//

#include <rdd_rpc.h>
#include <jubatus/msgpack/rpc/future.h>
#include "key_values_rdd_stub.h"

#include "rdd_context.h"

std::unique_ptr<KeyValuesRddStub> KeyValuesRddStub::Reduce(const std::string &dl_filename) {
  Shuffle();

  std::vector<msgpack::rpc::future> fs;
  int new_rdd_id = rc_->GetNewRddId();

  for (auto o : owners_) {
    fs.push_back(rc_->Call("reduce", o, rdd_id_, dl_filename, new_rdd_id));
  }

  for (auto f : fs) {
    if (f.get<rdd_rpc::Response>() != rdd_rpc::Response::OK) {
      return nullptr;
    }
  }

  return std::unique_ptr<KeyValuesRddStub>(new KeyValuesRddStub(rc_, new_rdd_id, owners_));
}

// shuffles rdds by pairwise algorithm
bool KeyValuesRddStub::Shuffle() {
  bool ret = true;
  std::vector<msgpack::rpc::future> fs;

  int n_steps = owners_.size();

  for (int step = 1; step < n_steps; step++) {
    for (auto owner : owners_) {
      int dest = owner ^step;
      if (dest > owner) {
        fs.push_back(rc_->Call("shuffle_srv", owner, rdd_id_, dest, owners_.size()));
      } else {
        fs.push_back(rc_->Call("shuffle_cli", owner, rdd_id_, rc_->GetSlaveAddrById(dest), dest, owners_.size()));
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
void KeyValuesRddStub::Print() {
  std::vector<msgpack::rpc::future> fs;

  for (auto o : owners_) {
    fs.push_back(rc_->Call("print", o, rdd_id_));
  }

  for (auto f : fs) {
    if (f.get<rdd_rpc::Response>() != rdd_rpc::Response::OK) {
      std::cout << "oops" << std::endl;
    }
  }
}
