//
// Created by Harunobu Daikoku on 2016/04/11.
//

#ifndef FULLY_CONNECTED_FULLY_CONNECTED_CLIENT_H
#define FULLY_CONNECTED_FULLY_CONNECTED_CLIENT_H

#include <algorithm>
#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <sys/socket.h>
#include <netdb.h>
#include <poll.h>
#include <queue>
#include <msgpack/sbuffer.hpp>
#include "shuffle/recv_buffer.h"
#include "slave/block_manager.h"
#include "socket/socket_client.h"

class FullyConnectedClient {
 public:
  FullyConnectedClient(const std::vector<std::pair<std::string, int>> &servers,
                       int my_owner_id,
                       BlockManager &block_mgr)
      : my_owner_id_(my_owner_id), block_mgr_(block_mgr) {
    for (const auto &server : servers) {
      clients_.emplace_back(new SocketClient(server.first, server.second));
    }
    num_clients_ = clients_.size();
  }

  std::thread Dispatch() {
    return std::thread([this]() {
      this->Run();
    });
  }

 private:
  static const int kMinBackoff = 1;
  static const int kMaxBackoff = 1024;
  int backoff_voted_;
  int num_clients_;
  int my_owner_id_;
  std::vector<std::unique_ptr<SocketClient>> clients_;
  BlockManager &block_mgr_;

  std::unordered_map<int, RecvBuffer> recv_buffers_;

  void Run();
  bool OnSend(struct pollfd &pfd, SocketClient &client);
  bool OnRecv(struct pollfd &pfd, SocketClient &client, RecvBuffer &rbuffer);
  void Close(struct pollfd &pfd);
  void ScheduleSend(struct pollfd &pfd);
  void ScheduleRecv(struct pollfd &pfd, int32_t size);

};

#endif //FULLY_CONNECTED_FULLY_CONNECTED_CLIENT_H
