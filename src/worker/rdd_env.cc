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

void RDDEnv::RegisterShuffleServer(std::unique_ptr<ShuffleService> shuffle_server) {
  shuffle_server->Start();
  shuffle_server_ = std::move(shuffle_server);
}

void RDDEnv::RegisterShuffleClient(std::unique_ptr<ShuffleService> shuffle_client) {
  shuffle_client->Start();
  shuffle_client_ = std::move(shuffle_client);
}

void RDDEnv::StopShuffleServices() {
  for (auto &service : shuffle_services_) {
    service->Stop();
  }
}