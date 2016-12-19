//
// Created by Harunobu Daikoku on 2016/12/16.
//

#ifndef PROJECT_PAIRWISE_SHUFFLE_SERVICE_H
#define PROJECT_PAIRWISE_SHUFFLE_SERVICE_H

#include "logger.h"
#include <worker_context.h>
#include "worker/rdd_env.h"
#include "worker/shuffle/block_manager.h"
#include "worker/net/socket_session_acceptor.h"
#include "worker/net/socket_session_pool.h"

class PairwiseShuffleService : public Logger {
 public:
  PairwiseShuffleService(int my_executor_id,
                         const std::vector<WorkerContext> &workers,
                         const std::unordered_map<int, std::vector<int>> &partition_ids)
      : Logger("PairwiseShuffleService"),
        my_executor_id_(my_executor_id),
        worker_contexts_(workers),
        partition_ids_(partition_ids) {}

  bool Init(SocketSessionAcceptor &acceptor);
  void Start(const std::unordered_map<int, std::vector<int>> &partition_ids);

 private:
  int my_executor_id_;
  std::vector<WorkerContext> worker_contexts_;
  std::unordered_map<int, std::vector<int>> partition_ids_;
  SocketSessionPool ssp_;

};

#endif //PROJECT_PAIRWISE_SHUFFLE_SERVICE_H
