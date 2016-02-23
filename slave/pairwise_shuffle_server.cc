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
  size_t len;
  while (true) {
    server.Read(sock_fd, &partition_id, sizeof(partition_id));
    if (partition_id == -1) {
      break;
    }
    auto block = server.ReadWithHeader(sock_fd, len);
    block_mgr_.UnpackBlocks(partition_id, block.get(), len);
  }

  msgpack::sbuffer sbuf;
  std::vector<std::unique_ptr<char[]>> refs;
  for (const auto &p : partition_ids) {
    server.Write(sock_fd, &p, sizeof(p));
    block_mgr_.PackBlocks(p, sbuf, refs);
    server.WriteWithHeader(sock_fd, sbuf.data(), sbuf.size());
    sbuf.clear();
  }
  partition_id = -1;
  server.Write(sock_fd, &partition_id, sizeof(partition_id));

}
