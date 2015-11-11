//
// Created by Harunobu Daikoku on 2015/11/05.
//

#ifndef SLAVERDD_TEXT_RDD_STUB_H
#define SLAVERDD_TEXT_RDD_STUB_H

#include <memory>
#include "rdd_stub.h"
#include "key_values_rdd_stub.h"

class TextRddStub: public RddStub {
 public:

  TextRddStub(RddContext *rc, int rdd_id, const std::set<int> &owners) : RddStub(rc, rdd_id, owners) { }

  std::unique_ptr<KeyValuesRddStub> Map(const std::string &dl_filename);
};


#endif //SLAVERDD_TEXT_RDD_STUB_H
