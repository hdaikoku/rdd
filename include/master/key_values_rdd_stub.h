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

  KeyValuesRDDStub(RDDContext *rc_, int rdd_id_, const std::set<int> &owners_) : RDDStub(rc_, rdd_id_, owners_) { }

  std::unique_ptr<KeyValueRDDStub> Reduce(const std::string &dl_filename);

  void Print();

 private:
  bool Shuffle();
};

#endif //SLAVERDD_KEY_VALUES_RDD_STUB_H
