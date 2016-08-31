//
// Created by Harunobu Daikoku on 2015/11/05.
//

#ifndef SLAVERDD_KEY_VALUES_RDD_STUB_H
#define SLAVERDD_KEY_VALUES_RDD_STUB_H

class KeyValueRDDStub;

#include <memory>

#include "rdd_stub.h"

class KeyValuesRDDStub: public RDDStub {

 public:

  KeyValuesRDDStub(RDDContext &rc,
                   int rdd_id,
                   const std::unordered_map<int, std::vector<int>> &partition_ids,
                   bool shuffled)
      : RDDStub(rc, rdd_id, partition_ids), shuffled_(shuffled) {}

  void GroupBy();

  std::unique_ptr<KeyValueRDDStub> Reduce(const std::string &dl_filename);

  bool Shuffle(const std::string &shuffle_type = "pairwise");

 private:
  bool shuffled_;

};

#endif //SLAVERDD_KEY_VALUES_RDD_STUB_H
