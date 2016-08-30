//
// Created by Harunobu Daikoku on 2016/01/11.
//

#include "worker/shuffle/pairwise_shuffle_server.h"

#include <iostream>

#include "worker/net/socket_server.h"

void PairwiseShuffleServer::Start(const std::vector<int> &partition_ids, const std::string &port) {
  SocketServer server(port);

  if (!server.Listen()) {
    std::cerr << "listen failed: " << port << std::endl;
    return;
  }

  auto sock = server.Accept();
  if (!sock) {
    perror("accept");
    return;
  }

  int len = 0;
  sock->Read(&len, sizeof(len));
  std::unique_ptr<char> rbuf(new char[len]);
  sock->Read(rbuf.get(), len);
  block_mgr_.GroupUnpackBlocks(rbuf.get(), len);

  msgpack::sbuffer sbuf;
  std::vector<std::unique_ptr<char[]>> refs;
  len = block_mgr_.GroupPackBlocks(partition_ids, sbuf, refs);
  sock->Write(&len, sizeof(len));
  sock->Write(sbuf.data(), len);
}
