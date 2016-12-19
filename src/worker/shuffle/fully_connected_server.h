//
// Created by Harunobu Daikoku on 2016/04/04.
//

#ifndef FULLY_CONNECTED_FULLY_CONNECTED_H
#define FULLY_CONNECTED_FULLY_CONNECTED_H

#include <thread>
#include "worker/rdd_env.h"
#include "worker/net/socket_non_blocking_server.h"
#include "worker/shuffle/block_manager.h"
#include "worker/shuffle/shuffle_service.h"

class FullyConnectedServer: public SocketNonBlockingServer, public ShuffleService {
 public:
  FullyConnectedServer(std::unordered_map<int, std::vector<int>> &partitions_by_owner)
      : block_mgr_(RDDEnv::GetInstance().GetBlockManager()),
        partitions_by_owner_(partitions_by_owner) { }

  virtual void Start() override;
  virtual void Stop() override;

 protected:
  virtual bool OnRecv(struct pollfd &pfd, const SocketCommon &socket) override;
  virtual bool OnSend(struct pollfd &pfd, const SocketCommon &socket, SendBuffer &send_buffer) override;
  void OnClose(struct pollfd &pfd) override;
  virtual bool IsRunning() override;

 private:
  BlockManager &block_mgr_;
  std::unordered_map<int, std::vector<int>> partitions_by_owner_;
  std::thread server_thread_;

};


#endif //FULLY_CONNECTED_FULLY_CONNECTED_H
