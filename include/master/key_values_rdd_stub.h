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

  KeyValuesRDDStub(RDDContext *rc, int rdd_id, const std::unordered_set<int> &owners, bool shuffled)
      : RDDStub(rc, rdd_id, owners), shuffled_(shuffled) { }

  std::unique_ptr<KeyValueRDDStub> Reduce(const std::string &dl_filename);

  void Print();

 private:
  bool shuffled_;
  bool Shuffle();
};

#endif //SLAVERDD_KEY_VALUES_RDD_STUB_H
