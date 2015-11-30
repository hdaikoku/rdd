//
// Created by Harunobu Daikoku on 2015/11/05.
//

#include <jubatus/msgpack/rpc/future.h>
#include "rdd_rpc.h"
#include "master/rdd_context.h"

std::unique_ptr<KeyValuesRDDStub> KeyValueRDDStub::Map(const std::string &dl_filename) {
  std::vector<msgpack::rpc::future> fs;
  int new_rdd_id = rc_->GetNewRddId();

  for (auto o : owners_) {
    rc_->SetTimeout(o, 60);
    fs.push_back(rc_->Call("map", o, rdd_id_, dl_filename, new_rdd_id));
  }

  for (auto f : fs) {
    if (f.get<rdd_rpc::Response>() != rdd_rpc::Response::OK) {
      return nullptr;
    }
  }

  return std::unique_ptr<KeyValuesRDDStub>(new KeyValuesRDDStub(rc_, new_rdd_id, owners_));
}

std::unique_ptr<KeyValuesRDDStub> KeyValueRDDStub::Map(const std::string &dl_mapper, const std::string &dl_combiner) {
  auto key_values = Map(dl_mapper);

  key_values->Combine(dl_combiner);

  return key_values;
}


void KeyValueRDDStub::Print() {
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