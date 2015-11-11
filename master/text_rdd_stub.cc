//
// Created by Harunobu Daikoku on 2015/11/05.
//

#include <jubatus/msgpack/rpc/future.h>
#include "rdd_rpc.h"
#include "text_rdd_stub.h"

#include "rdd_context.h"

std::unique_ptr<KeyValuesRddStub> TextRddStub::Map(const std::string &dl_filename) {
  std::vector<msgpack::rpc::future> fs;
  int new_rdd_id = rc_->GetNewRddId();

  for (auto o : owners_) {
    fs.push_back(rc_->Call("map", o, rdd_id_, dl_filename, new_rdd_id));
  }

  for (auto f : fs) {
    if (f.get<rdd_rpc::Response>() != rdd_rpc::Response::OK) {
      return nullptr;
    }
  }

  return std::unique_ptr<KeyValuesRddStub>(new KeyValuesRddStub(rc_, new_rdd_id, owners_));
}
