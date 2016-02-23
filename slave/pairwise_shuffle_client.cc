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
  size_t len;
  msgpack::sbuffer sbuf;
  std::vector<std::unique_ptr<char[]>> refs;
  for (const auto &p : partition_ids) {
    client.Write(sock_fd, &p, sizeof(p));
    block_mgr_.PackBlocks(p, sbuf, refs);
    len = sbuf.size();
    client.Write(sock_fd, &len, sizeof(len));
    client.Write(sock_fd, sbuf.data(), len);
    sbuf.clear();
  }
  partition_id = -1;
  client.Write(sock_fd, &partition_id, sizeof(partition_id));

  std::vector<char> rbuf(1 << 19);
  while (true) {
    client.Read(sock_fd, &partition_id, sizeof(partition_id));
    if (partition_id == -1) {
      break;
    }
    client.Read(sock_fd, &len, sizeof(len));
    if (rbuf.capacity() < len) {
      rbuf.resize(len);
    }
    client.Read(sock_fd, rbuf.data(), len);
    block_mgr_.UnpackBlocks(partition_id, rbuf.data(), len);
  }

}

