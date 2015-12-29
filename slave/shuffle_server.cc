//
// Created by Harunobu Daikoku on 2015/12/24.
//

#include <iostream>
#include "shuffle_server.h"

void ShuffleServer::Dispatch() {
  if (!server_.Listen()) {
    std::cerr << "listen failed: " << std::endl;
    return;
  }

  int sock_fd, finished = 0;
  while (true) {
    if (finished == n_dests_) {
      break;
    }
    if ((sock_fd = server_.Accept()) < 0) {
      perror("accept");
      return;
    }

    int client_id = 0;
    if (server_.Read(sock_fd, &client_id, sizeof(client_id)) < 0) {
      std::cerr << "read failed" << std::endl;
      continue;
    }

    long block_len = 0;
    auto block = block_mgr_.GetBlock(client_id, block_len);

    std::string header(std::to_string(block_len) + "\r\n");
    server_.Write(sock_fd, header.c_str(), 16);
    if (block_len > 0) {
      server_.Write(sock_fd, block.get(), block_len);
    }

    close(sock_fd);
    if (block_len == -1) {
      // TODO
      finished++;
    }
  }

}
