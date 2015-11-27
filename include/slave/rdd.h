//
// Created by Harunobu Daikoku on 2015/11/05.
//

#ifndef SLAVERDD_RDD_H
#define SLAVERDD_RDD_H

#include <string>

class RDD {
 public:
  RDD() { }
  virtual ~RDD() { }

  virtual void Print() = 0;

 protected:
  void *LoadLib(const std::string &dl_filename);
  void *LoadFunc(void *handle, const std::string &func_name);

};


#endif //SLAVERDD_RDD_H
