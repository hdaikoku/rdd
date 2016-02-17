//
// Created by Harunobu Daikoku on 2015/11/05.
//

#ifndef SLAVERDD_KEY_VALUE_RDD_STUB_H
#define SLAVERDD_KEY_VALUE_RDD_STUB_H

#include <memory>
#include "rdd_stub.h"
#include "key_values_rdd_stub.h"

class KeyValueRDDStub: public RDDStub {
 public:

  KeyValueRDDStub(RDDContext& rc, int rdd_id, const std::unordered_set<int> &owners) : RDDStub(rc, rdd_id, owners) { }

  std::unique_ptr<KeyValuesRDDStub> Map(const std::string &dl_filename);

  std::unique_ptr<KeyValuesRDDStub> Map(const std::string &dl_mapper, const std::string &dl_combiner, bool overlap = true);

  void Print();
};


#endif //SLAVERDD_KEY_VALUE_RDD_STUB_H
