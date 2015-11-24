//
// Created by Harunobu Daikoku on 2015/11/05.
//

#ifndef SLAVERDD_RDD_STUB_H
#define SLAVERDD_RDD_STUB_H

class RDDContext;

#include <set>

class RDDStub {

 public:

  RDDStub(RDDContext *rc_, int rdd_id_, const std::set<int> &owners_)
      : rdd_id_(rdd_id_), owners_(owners_), rc_(rc_) { }

 protected:
  int rdd_id_;
  std::set<int> owners_;
  RDDContext *rc_;
};


#endif //SLAVERDD_RDD_STUB_H
