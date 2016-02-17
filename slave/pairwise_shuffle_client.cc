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

  msgpack::sbuffer sbuf;
  std::vector<std::unique_ptr<char[]>> refs;
  for (const auto &p : partition_ids) {
    block_mgr_.PackBlocks(p, sbuf, refs);
  }

  if (client.WriteWithHeader(sock_fd, sbuf.data(), sbuf.size()) < 0) {
    std::cerr << "write failed" << std::endl;
    return;
  }
  free(sbuf.release());

  size_t len = 0;
  auto rbuf = client.ReadWithHeader(sock_fd, len);
  if (!rbuf) {
    std::cerr << "read failed" << std::endl;
    return;
  }

  block_mgr_.UnpackBlocks(rbuf.get(), len);
}

