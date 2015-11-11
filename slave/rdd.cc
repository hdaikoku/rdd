//
// Created by Harunobu Daikoku on 2015/11/05.
//

#include <dlfcn.h>
#include <iostream>
#include "rdd.h"

void *Rdd::LoadLib(const std::string &dl_filename) {
  const auto lib = dlopen(dl_filename.c_str(), RTLD_LAZY);
  if (!lib) {
    std::cerr << "Cannot load library: " << dlerror() << std::endl;
  }

  return lib;
}

void *Rdd::LoadFunc(void *handle, const std::string &func_name) {
  const auto func = dlsym(handle, func_name.c_str());
  const auto dlsym_error = dlerror();
  if (dlsym_error) {
    std::cerr << "Cannot load symbol create: " << dlsym_error << std::endl;
    dlclose(handle);
    return nullptr;
  }

  return func;
}
