//
// Created by Harunobu Daikoku on 2015/11/05.
//

#include <jubatus/msgpack/rpc/future.h>
#include "rdd_rpc.h"
#include "master/rdd_context.h"

std::unique_ptr<KeyValuesRDDStub> KeyValueRDDStub::Map(const std::string &dl_mapper) {
  std::vector<msgpack::rpc::future> fs;
  int new_rdd_id = rc_->GetNewRddId();

  for (auto o : owners_) {
    rc_->SetTimeout(o, 600);
    fs.push_back(rc_->Call("map", o, rdd_id_, dl_mapper, new_rdd_id));
  }

  for (auto f : fs) {
    if (f.get<rdd_rpc::Response>() != rdd_rpc::Response::OK) {
      return nullptr;
    }
  }

  std::unique_ptr<KeyValuesRDDStub> mapped(new KeyValuesRDDStub(rc_, new_rdd_id, owners_));
  mapped->Shuffle();

  return std::move(mapped);
}

std::unique_ptr<KeyValuesRDDStub> KeyValueRDDStub::Map(const std::string &dl_mapper,
                                                       const std::string &dl_combiner,
                                                       bool overlap) {
  std::vector<msgpack::rpc::future> fs;
  int new_rdd_id = rc_->GetNewRddId();


  for (auto o : owners_) {
    rc_->SetTimeout(o, 600);
    if (overlap) {
      std::vector<int> owners(owners_.begin(), owners_.end());
      fs.push_back(rc_->Call("map_with_shuffle", o, rdd_id_, dl_mapper, dl_combiner, owners, new_rdd_id));
    } else {
      fs.push_back(rc_->Call("map_with_combine", o, rdd_id_, dl_mapper, dl_combiner, new_rdd_id));
    }

  }

  for (auto f : fs) {
    if (f.get<rdd_rpc::Response>() != rdd_rpc::Response::OK) {
      return nullptr;
    }
  }

  std::unique_ptr<KeyValuesRDDStub> mapped(new KeyValuesRDDStub(rc_, new_rdd_id, owners_));

  if (!overlap) {
    mapped->Shuffle();
  }

  return std::move(mapped);
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