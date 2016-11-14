//
// Created by Harunobu Daikoku on 2015/11/05.
//

#include <rdd_context.h>
#include <stdlib.h>
#include "rdd_rpc.h"

std::unique_ptr<KeyValuesRDDStub> KeyValueRDDStub::Map(const std::string &dl_mapper,
                                                       const std::string &dl_combiner) {
  std::vector<msgpack::rpc::future> fs;

  int new_rdd_id = rc_.GetNewRddId();
  std::vector<int> owners;
  GetOwners(owners);
  auto overlap = rc_.OverlapShuffle();
  std::string dl_mapper_path(realpath(dl_mapper.c_str(), NULL));
  std::string dl_combiner_path;
  if (dl_combiner != "") {
    dl_combiner_path.assign(realpath(dl_combiner.c_str(), NULL));
  } else {
    dl_combiner_path.assign(dl_combiner);
  }

  if (overlap) {
    StartShuffleService();
  }

  for (const auto &p : partitions_by_owner_) {
    rc_.SetTimeout(p.first, 600);
    fs.push_back(rc_.Call("map", p.first, rdd_id_, dl_mapper_path, dl_combiner_path, new_rdd_id));
  }

  for (auto &f : fs) {
    if (f.get<rdd_rpc::Response>() != rdd_rpc::Response::OK) {
      return nullptr;
    }
  }

  if (overlap) {
    StopShuffleService();
  }

  std::unique_ptr<KeyValuesRDDStub> mapped(new KeyValuesRDDStub(rc_, new_rdd_id, partitions_by_owner_, overlap));

  return std::move(mapped);
}
