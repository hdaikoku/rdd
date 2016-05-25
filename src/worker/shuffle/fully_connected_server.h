//
// Created by Harunobu Daikoku on 2016/04/04.
//

#ifndef FULLY_CONNECTED_FULLY_CONNECTED_H
#define FULLY_CONNECTED_FULLY_CONNECTED_H

#include <thread>
#include "worker/shuffle/block_manager.h"
#include "worker/shuffle/socket/socket_non_blocking_server.h"

class FullyConnectedServer: public SocketNonBlockingServer {
 public:
  FullyConnectedServer(const int server_port,
                       BlockManager &block_mgr,
                       std::unordered_map<int, std::vector<int>> &partitions_by_owner)
      : SocketNonBlockingServer(server_port), block_mgr_(block_mgr),
        partitions_by_owner_(partitions_by_owner) { }

  std::thread Dispatch() {
    return std::thread([this]() {
      this->Run();
    });
  }

 protected:
  virtual bool OnRecv(struct pollfd &pfd) override;
  virtual bool OnSend(struct pollfd &pfd, SendBuffer &send_buffer) override;
  virtual bool OnClose(struct pollfd &pfd) override;
  virtual bool IsRunning() override;


 private:
  BlockManager &block_mgr_;
  std::unordered_map<int, std::vector<int>> partitions_by_owner_;
};


#endif //FULLY_CONNECTED_FULLY_CONNECTED_H
