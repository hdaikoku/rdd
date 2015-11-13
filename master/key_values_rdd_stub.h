//
// Created by Harunobu Daikoku on 2015/11/05.
//

#ifndef SLAVERDD_KEY_VALUES_RDD_STUB_H
#define SLAVERDD_KEY_VALUES_RDD_STUB_H


#include <memory>
#include "rdd_stub.h"
class KeyValuesRddStub: public RddStub {

 public:

  KeyValuesRddStub(RddContext *rc_, int rdd_id_, const std::set<int> &owners_) : RddStub(rc_, rdd_id_, owners_) { }

  std::unique_ptr<KeyValuesRddStub> Reduce(const std::string &dl_filename);

  void Print();

 private:
  bool Shuffle();
};


#endif //SLAVERDD_KEY_VALUES_RDD_STUB_H
