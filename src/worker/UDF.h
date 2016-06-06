//
// Created by Harunobu Daikoku on 2016/06/06.
//

#ifndef PROJECT_UDF_H
#define PROJECT_UDF_H

#include <dlfcn.h>
#include <iostream>
#include <string>

class UDF {
 public:
  UDF(const std::string &dl_filename) {
    lib_ = dlopen(dl_filename.c_str(), RTLD_LAZY);
    if (!lib_) {
      std::cerr << "Cannot load library: " << dlerror() << std::endl;
    }
  }

  virtual ~UDF() {
    if (lib_) {
      dlclose(lib_);
    }
  }

  template<typename T>
  T LoadFunc(const std::string &func_name) {
    if (!lib_) {
      return nullptr;
    }

    const auto func = dlsym(lib_, func_name.c_str());
    const auto dlsym_error = dlerror();
    if (dlsym_error) {
      std::cerr << "Cannot load symbol create: " << dlsym_error << std::endl;
      dlclose(lib_);
      return nullptr;
    }

    return reinterpret_cast<T>(func);
  }

 private:
  void *lib_;

};

#endif //PROJECT_UDF_H
