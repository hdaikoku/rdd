//
// Created by Harunobu Daikoku on 2016/01/11.
//

#include <cassert>
#include <iostream>
#include "slave/pairwise_shuffle_server.h"
#include "socket/socket_server.h"

void PairwiseShuffleServer::Start(const std::vector<int> &partition_ids, int port) {
  int sock_fd;
  SocketServer server(std::to_string(port));

  if (!server.Listen()) {
    std::cerr << "listen failed: " << port << std::endl;
    return;
  }
  std::cout << "listening: " << port << std::endl;

  if ((sock_fd = server.Accept()) < 0) {
    perror("accept");
    return;
  }

  int partition_id;
  int32_t len = -1;
  while (true) {
    server.Read(sock_fd, &partition_id, sizeof(partition_id));
    if (partition_id == -1) {
      break;
    }
    while (true) {
      server.Read(sock_fd, &len, sizeof(len));
      if (len == -1) {
        break;
      }
      assert(len >= 0);
      std::unique_ptr<char[]> buf(new char[len]);
      server.Read(sock_fd, buf.get(), static_cast<size_t>(len));
      block_mgr_.PutBlock(partition_id, len, std::move(buf));
    }
  }

  for (const auto &p : partition_ids) {
    server.Write(sock_fd, &p, sizeof(p));
    while (true) {
      auto block = block_mgr_.GetBlock(p, len);
      server.Write(sock_fd, &len, sizeof(len));
      if (len == -1) {
        break;
      }
      server.Write(sock_fd, block.get(), static_cast<size_t>(len));
    }
  }
  partition_id = -1;
  server.Write(sock_fd, &partition_id, sizeof(partition_id));
}
