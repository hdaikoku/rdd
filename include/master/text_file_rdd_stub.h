//
// Created by Harunobu Daikoku on 2016/02/17.
//

#ifndef PROJECT_TEXT_FILE_RDD_STUB_H
#define PROJECT_TEXT_FILE_RDD_STUB_H

#include <unordered_set>
#include "text_file_index.h"
#include "master/key_value_rdd_stub.h"

class TextFileRDDStub: public KeyValueRDDStub {
 public:

  static std::unique_ptr<TextFileRDDStub> NewInstance(RDDContext &rc, const std::string &filename);

 private:
  TextFileRDDStub(RDDContext &rc, int rdd_id)
      : KeyValueRDDStub(rc, rdd_id) { }

};

#endif //PROJECT_TEXT_FILE_RDD_STUB_H
