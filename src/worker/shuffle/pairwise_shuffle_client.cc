//
// Created by Harunobu Daikoku on 2016/01/11.
//

#include "worker/shuffle/pairwise_shuffle_client.h"

#include <iostream>

#include "worker/net/socket_client.h"

void PairwiseShuffleClient::Start(const std::vector<int> &partition_ids,
                                  const std::string &server_addr,
                                  const std::string &server_port) {
  SocketClient client(server_addr, server_port);

  if (client.Connect() < 0) {
    std::cerr << "could not connect to: " << server_addr << ":" << server_port << std::endl;
    return;
  }

  msgpack::sbuffer sbuf;
  std::vector<std::unique_ptr<char[]>> refs;
  auto len = block_mgr_.GroupPackBlocks(partition_ids, sbuf, refs);
  client.Write(&len, sizeof(len));
  client.Write(sbuf.data(), len);

  client.Read(&len, sizeof(len));
  std::unique_ptr<char> rbuf(new char[len]);
  client.Read(rbuf.get(), len);
  block_mgr_.GroupUnpackBlocks(rbuf.get(), len);
}