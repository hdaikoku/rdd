//
// Created by Harunobu Daikoku on 2016/01/11.
//

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

  size_t len = 0;
  auto rbuf = server.ReadWithHeader(sock_fd, len);
  if (!rbuf) {
    std::cerr << "read failed" << std::endl;
    return;
  }

  block_mgr_.UnpackBlocks(rbuf.get(), len);

  msgpack::sbuffer sbuf;
  std::vector<std::unique_ptr<char[]>> refs;
  for (const auto &p : partition_ids) {
    block_mgr_.PackBlocks(p, sbuf, refs);
  }

  if (server.WriteWithHeader(sock_fd, sbuf.data(), sbuf.size()) < 0) {
    std::cerr << "write failed" << std::endl;
    return;
  }
}
