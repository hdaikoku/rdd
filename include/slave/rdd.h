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

  inline std::string to_string(const long long int &s) const {
    return std::to_string(s);
  }

  inline std::string to_string(const std::string &s) const {
    return s;
  }

  template <typename T1, typename T2>
  inline std::string to_string(const std::pair<T1, T2> &p) const  {
    return (p.first + ", " + p.second);
  }

};


#endif //SLAVERDD_RDD_H
