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

  size_t len;
  msgpack::sbuffer sbuf;
  std::vector<std::unique_ptr<char[]>> refs;
  for (const auto &p : partition_ids) {
    block_mgr_.PackBlocks(p, sbuf, refs);
  }
  client.WriteWithHeader(sock_fd, sbuf.data(), sbuf.size());


  auto blocks = client.ReadWithHeader(sock_fd, len);
  block_mgr_.UnpackBlocks(0, blocks.get(), len);

}

