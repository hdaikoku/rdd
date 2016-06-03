//
// Created by Harunobu Daikoku on 2016/01/11.
//

#include "worker/shuffle/pairwise_shuffle_server.h"

#include <iostream>

#include "worker/shuffle/socket/socket_server.h"

void PairwiseShuffleServer::Start(const std::vector<int> &partition_ids, int port) {
  int sock_fd;
  SocketServer server(port);

  if (!server.Listen()) {
    std::cerr << "listen failed: " << port << std::endl;
    return;
  }

  if ((sock_fd = server.Accept(server.GetListenSocket())) < 0) {
    perror("accept");
    return;
  }

  int len = 0;
  server.Read(sock_fd, &len, sizeof(len));
  std::unique_ptr<char> rbuf(new char[len]);
  server.Read(sock_fd, rbuf.get(), len);
  block_mgr_.GroupUnpackBlocks(rbuf.get(), len);

  msgpack::sbuffer sbuf;
  std::vector<std::unique_ptr<char[]>> refs;
  len = block_mgr_.GroupPackBlocks(partition_ids, sbuf, refs);
  server.Write(sock_fd, &len, sizeof(len));
  server.Write(sock_fd, sbuf.data(), len);

}
