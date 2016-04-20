//
// Created by Harunobu Daikoku on 2016/04/04.
//

#ifndef FULLY_CONNECTED_FULLY_CONNECTED_H
#define FULLY_CONNECTED_FULLY_CONNECTED_H

#include "slave/block_manager.h"
#include "socket/non_blocking_server.h"

class FullyConnectedServer: public NonBlockingServer {
 public:
  FullyConnectedServer(const int server_port, BlockManager &block_mgr, int num_clients)
      : NonBlockingServer(server_port), block_mgr_(block_mgr),
        num_clients_(num_clients), num_completed_(0) { }

  std::thread Dispatch() {
    return std::thread([this]() {
      this->Run();
    });
  }

 protected:
  virtual bool OnRecv(struct pollfd &pfd) override;
  virtual bool OnSend(struct pollfd &pfd, SendBuffer &send_buffer) override;
  virtual bool IsRunning() override;


 private:
  BlockManager &block_mgr_;
  int num_clients_;
  int num_completed_;
};


#endif //FULLY_CONNECTED_FULLY_CONNECTED_H
