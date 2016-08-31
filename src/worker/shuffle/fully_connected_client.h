//
// Created by Harunobu Daikoku on 2016/04/11.
//

#ifndef FULLY_CONNECTED_FULLY_CONNECTED_CLIENT_H
#define FULLY_CONNECTED_FULLY_CONNECTED_CLIENT_H

#include <thread>
#include <vector>
#include "worker/rdd_env.h"
#include "worker/net/socket_client_pool.h"
#include "worker/shuffle/block_manager.h"
#include "worker/shuffle/shuffle_service.h"

class FullyConnectedClient: public SocketClientPool, public ShuffleService {
 public:
  FullyConnectedClient(const std::vector<std::pair<std::string, std::string>> &servers,
                       int my_owner_id)
      : SocketClientPool(servers), my_owner_id_(my_owner_id), block_mgr_(RDDEnv::GetInstance().GetBlockManager()) {}

  virtual void Start() override;

  virtual void Stop() override;

 private:
  static const int kTagHeader = 1;
  static const int kTagBody = 2;
  int my_owner_id_;
  BlockManager &block_mgr_;
  std::thread client_thread_;

 protected:
  virtual bool OnRecv(struct pollfd &pfd, const SocketCommon &socket, RecvBuffer &rbuffer) override;
  virtual bool OnSend(struct pollfd &pfd, const SocketCommon &socket) override;

};

#endif //FULLY_CONNECTED_FULLY_CONNECTED_CLIENT_H
