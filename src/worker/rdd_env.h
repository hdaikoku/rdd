//
// Created by Harunobu Daikoku on 2016/07/20.
//

#ifndef PROJECT_RDD_ENV_H
#define PROJECT_RDD_ENV_H

#include "worker/shuffle/shuffle_service.h"
#include "worker/shuffle/block_manager.h"

class RDDEnv {
 public:
  RDDEnv(const RDDEnv &) = delete;
  RDDEnv(RDDEnv &&) = delete;

  RDDEnv &operator=(const RDDEnv &) = delete;

  static RDDEnv &GetInstance();

  BlockManager &GetBlockManager();

  void RegisterShuffleService(std::unique_ptr<ShuffleService> shuffle_service);

  void StopShuffleServices();

 private:
  RDDEnv() = default;
  ~RDDEnv() = default;

  BlockManager block_manager_;
  std::vector<std::unique_ptr<ShuffleService>> shuffle_services_;

};


#endif //PROJECT_RDD_ENV_H
