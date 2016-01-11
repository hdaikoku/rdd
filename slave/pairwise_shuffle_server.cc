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

  UnpackBlocks(client_id, rbuf.get(), len);

  msgpack::sbuffer sbuf;
  PackBlocks(client_id, sbuf);

  if (server.WriteWithHeader(sock_fd, sbuf.data(), sbuf.size()) < 0) {
    std::cerr << "write failed" << std::endl;
    return;
  }
}

void PairwiseShuffleServer::PackBlocks(int client_id, msgpack::sbuffer &sbuf) {
  long len = 0;
  while (true) {
    auto block = block_mgr_.GetBlock(client_id, len);
    if (len == 0) {
      break;
    }
    msgpack::pack(&sbuf, std::string(block.get(), len));
  }
}

void PairwiseShuffleServer::UnpackBlocks(int client_id, const char *buf, long len) {
  msgpack::unpacker upc;
  upc.reserve_buffer(len);
  memcpy(upc.buffer(), buf, len);
  upc.buffer_consumed(len);

  msgpack::unpacked result;
  while (upc.next(&result)) {
    std::string received;
    result.get().convert(&received);
    std::unique_ptr<char[]> block(new char[len]);
    received.copy(block.get(), len);
    block_mgr_.PutBlock(client_id, received.length(), std::move(block));
  }
}
