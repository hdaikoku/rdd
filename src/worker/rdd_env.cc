//
// Created by Harunobu Daikoku on 2016/07/20.
//

#include "rdd_env.h"

RDDEnv &RDDEnv::GetInstance() {
  static RDDEnv rdd_env;
  return rdd_env;
}

BlockManager &RDDEnv::GetBlockManager() {
  return block_manager_;
}

void RDDEnv::RegisterShuffleService(std::unique_ptr<ShuffleService> shuffle_service) {
  shuffle_service->Start();
  shuffle_services_.push_back(std::move(shuffle_service));
}

void RDDEnv::StopShuffleServices() {
  for (auto &service : shuffle_services_) {
    service->Stop();
  }
}