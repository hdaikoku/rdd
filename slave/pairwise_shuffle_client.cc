//
// Created by Harunobu Daikoku on 2016/01/11.
//

#include <iostream>
#include <cassert>
#include "socket/socket_client.h"
#include "slave/pairwise_shuffle_client.h"


void PairwiseShuffleClient::Start(const std::vector<int> &partition_ids,
                                  const std::string &server_addr,
                                  int server_port) {
  int sock_fd;
  SocketClient client(server_addr, std::to_string(server_port));
  std::cout << "connecting to " << server_addr << ":" << server_port << std::endl;

  if ((sock_fd = client.Connect()) < 0) {
    std::cerr << "could not connect to: " << server_addr << ":" << server_port << std::endl;
    return;
  }

  int partition_id;
  int32_t len = -1;
  for (const auto &p : partition_ids) {
    client.Write(sock_fd, &p, sizeof(p));
    while (true) {
      auto block = block_mgr_.GetBlock(p, len);
      client.Write(sock_fd, &len, sizeof(len));
      if (len == -1) {
        break;
      }
      client.Write(sock_fd, block.get(), static_cast<size_t>(len));
    }
  }
  partition_id = -1;
  client.Write(sock_fd, &partition_id, sizeof(partition_id));

  while (true) {
    client.Read(sock_fd, &partition_id, sizeof(partition_id));
    if (partition_id == -1) {
      break;
    }
    while (true) {
      client.Read(sock_fd, &len, sizeof(len));
      if (len == -1) {
        break;
      }
      assert(len > 0);
      std::unique_ptr<char[]> buf(new char[len]);
      client.Read(sock_fd, buf.get(), static_cast<size_t>(len));
      block_mgr_.PutBlock(partition_id, len, std::move(buf));
    }
  }

}

