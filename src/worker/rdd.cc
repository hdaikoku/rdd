//
// Created by Harunobu Daikoku on 2015/11/05.
//

#include "worker/rdd.h"

#include <dlfcn.h>
#include <iostream>

void *RDD::LoadLib(const std::string &dl_filename) {
  const auto lib = dlopen(dl_filename.c_str(), RTLD_LAZY);
  if (!lib) {
    std::cerr << "Cannot load library: " << dlerror() << std::endl;
  }

  return lib;
}

void *RDD::LoadFunc(void *handle, const std::string &func_name) {
  const auto func = dlsym(handle, func_name.c_str());
  const auto dlsym_error = dlerror();
  if (dlsym_error) {
    std::cerr << "Cannot load symbol create: " << dlsym_error << std::endl;
    dlclose(handle);
    return nullptr;
  }

  return func;
}

void RDD::CloseLib(void *handle) {
  dlclose(handle);
}

int RDD::GetNumPartitions() const {
  return num_partitions_;
}

int RDD::GetPartitionID() const {
  return partition_id_;
}
