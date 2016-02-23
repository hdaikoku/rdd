//
// Created by Harunobu Daikoku on 2016/01/11.
//

#include <iostream>
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
  msgpack::sbuffer sbuf;
  std::vector<std::unique_ptr<char[]>> refs;
  for (const auto &p : partition_ids) {
    client.Write(sock_fd, &p, sizeof(p));
    block_mgr_.PackBlocks(p, sbuf, refs);
    client.WriteWithHeader(sock_fd, sbuf.data(), sbuf.size());
    sbuf.clear();
  }
  partition_id = -1;
  client.Write(sock_fd, &partition_id, sizeof(partition_id));

  size_t len;
  while (true) {
    client.Read(sock_fd, &partition_id, sizeof(partition_id));
    if (partition_id == -1) {
      break;
    }
    auto block = client.ReadWithHeader(sock_fd, len);
    block_mgr_.UnpackBlocks(partition_id, block.get(), len);
  }

}

