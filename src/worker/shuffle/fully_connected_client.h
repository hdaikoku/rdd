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

class FullyConnectedClient: public SocketClientPool {
 public:
  FullyConnectedClient(const std::vector<std::pair<std::string, std::string>> &servers,
                       int my_owner_id)
      : servers_(servers), my_owner_id_(my_owner_id), block_mgr_(RDDEnv::GetInstance().GetBlockManager()) {}

  std::thread Dispatch() {
    return std::thread([this]() {
      this->Run(servers_);
    });
  }

 private:
  int my_owner_id_;
  std::vector<std::pair<std::string, std::string>> servers_;
  BlockManager &block_mgr_;

 protected:
  virtual bool OnRecv(struct pollfd &pfd, const SocketCommon &socket, RecvBuffer &rbuffer) override;
  virtual bool OnSend(struct pollfd &pfd, const SocketCommon &socket) override;

 private:
  static const int kTagHeader = 1;
  static const int kTagBody = 2;

};

#endif //FULLY_CONNECTED_FULLY_CONNECTED_CLIENT_H
