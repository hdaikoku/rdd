//
// Created by Harunobu Daikoku on 2015/11/05.
//

#ifndef SLAVERDD_RDD_H
#define SLAVERDD_RDD_H

#include <memory>
#include <map_reduce.h>

class Rdd {
 public:
  Rdd() { }
  virtual ~Rdd() { }
  //virtual std::unique_ptr<KeyValuesRdd<>> Map(const std::string &dl_filename) = 0;
  //virtual std::unique_ptr<KeyValuesRdd<>> Reduce(const std::string &dl_filename) = 0;

 protected:
  void *LoadLib(const std::string &dl_filename);
  void *LoadFunc(void *handle, const std::string &func_name);

};


#endif //SLAVERDD_RDD_H
