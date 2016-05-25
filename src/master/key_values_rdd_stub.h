//
// Created by Harunobu Daikoku on 2015/11/05.
//

#ifndef SLAVERDD_KEY_VALUES_RDD_STUB_H
#define SLAVERDD_KEY_VALUES_RDD_STUB_H

class KeyValueRDDStub;

#include <memory>

#include "master/rdd_stub.h"

class KeyValuesRDDStub: public RDDStub {

 public:

  KeyValuesRDDStub(RDDContext &rc, int rdd_id, const std::unordered_map<int, std::vector<int>> &partition_ids)
      : RDDStub(rc, rdd_id, partition_ids) { }

  void GroupBy();

  std::unique_ptr<KeyValueRDDStub> Reduce(const std::string &dl_filename);

  bool Shuffle();

};

#endif //SLAVERDD_KEY_VALUES_RDD_STUB_H
