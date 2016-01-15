//
// Created by Harunobu Daikoku on 2016/01/11.
//

#include <iostream>
#include "pairwise_shuffle_server.h"
#include "socket/socket_server.h"


void PairwiseShuffleServer::Start(int client_id, int port) {
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

  UnpackBlocks(rbuf.get(), len);

  msgpack::sbuffer sbuf;
  PackBlocks(client_id, sbuf);

  if (server.WriteWithHeader(sock_fd, sbuf.data(), sbuf.size()) < 0) {
    std::cerr << "write failed" << std::endl;
    return;
  }
}

void PairwiseShuffleServer::PackBlocks(int client_rank, msgpack::sbuffer &sbuf) {
  long len = 0;
  while (true) {
    auto block = block_mgr_.GetBlock(client_rank, len);
    if (len == -1) {
      break;
    }
    msgpack::pack(&sbuf, std::string(block.get(), len));
  }
}

void PairwiseShuffleServer::UnpackBlocks(const char *buf, long len) {
  msgpack::unpacker upc;
  upc.reserve_buffer(len);
  memcpy(upc.buffer(), buf, len);
  upc.buffer_consumed(len);

  msgpack::unpacked result;
  while (upc.next(&result)) {
    std::string received;
    result.get().convert(&received);
    auto block_size = received.length();
    std::unique_ptr<char[]> block(new char[block_size]);
    received.copy(block.get(), block_size);
    block_mgr_.PutBlock(my_rank_, block_size, std::move(block));
  }
}
