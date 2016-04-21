//
// Created by Harunobu Daikoku on 2016/01/11.
//

#include <cassert>
#include <iostream>
#include "shuffle/pairwise_shuffle_server.h"
#include "socket/socket_server.h"

void PairwiseShuffleServer::Start(const std::vector<int> &partition_ids, int port) {
  int sock_fd;
  SocketServer server(port);

  if (!server.Listen()) {
    std::cerr << "listen failed: " << port << std::endl;
    return;
  }
  std::cout << "listening: " << port << std::endl;

  if ((sock_fd = server.Accept(server.GetListenSocket())) < 0) {
    perror("accept");
    return;
  }

  int partition_id;
  size_t len;
  std::vector<char> rbuf(1 << 19);
  while (true) {
    server.Read(sock_fd, &partition_id, sizeof(partition_id));
    if (partition_id == -1) {
      break;
    }
    server.Read(sock_fd, &len, sizeof(len));
    if (rbuf.capacity() < len) {
      rbuf.resize(len);
    }
    server.Read(sock_fd, rbuf.data(), len);
    block_mgr_.UnpackBlocks(partition_id, rbuf.data(), len);
  }

  msgpack::sbuffer sbuf;
  std::vector<std::unique_ptr<char[]>> refs;
  for (const auto &p : partition_ids) {
    server.Write(sock_fd, &p, sizeof(p));
    block_mgr_.PackBlocks(p, sbuf, refs);
    len = sbuf.size();
    server.Write(sock_fd, &len, sizeof(len));
    server.Write(sock_fd, sbuf.data(), len);
    sbuf.clear();
  }
  partition_id = -1;
  server.Write(sock_fd, &partition_id, sizeof(partition_id));

}