//
// Created by Harunobu Daikoku on 2015/11/05.
//

#ifndef SLAVERDD_KEY_VALUE_RDD_STUB_H
#define SLAVERDD_KEY_VALUE_RDD_STUB_H

#include <memory>
#include "rdd_stub.h"
#include "rdd_context.h"
#include "key_values_rdd_stub.h"

class KeyValueRDDStub: public RDDStub {
 public:

  KeyValueRDDStub(RDDContext &rc, int rdd_id) : RDDStub(rc, rdd_id) { }

  KeyValueRDDStub(RDDContext &rc, int rdd_id, const std::unordered_map<int, std::vector<int>> &partition_ids)
      : RDDStub(rc, rdd_id, partition_ids) { }

  std::unique_ptr<KeyValuesRDDStub>
      Map(const std::string &dl_mapper, const std::string &dl_combiner = "", bool overlap = true);

};

#endif //SLAVERDD_KEY_VALUE_RDD_STUB_H
