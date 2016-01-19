//
// Created by Harunobu Daikoku on 2016/01/11.
//

#include <iostream>
#include "slave/pairwise_shuffle_server.h"
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
  std::vector<std::unique_ptr<char[]>> refs;
  PackBlocks(client_id, sbuf, refs);

  if (server.WriteWithHeader(sock_fd, sbuf.data(), sbuf.size()) < 0) {
    std::cerr << "write failed" << std::endl;
    return;
  }
}

void PairwiseShuffleServer::PackBlocks(int client_rank,
                                       msgpack::sbuffer &sbuf,
                                       std::vector<std::unique_ptr<char[]>> &refs) {
  long len = 0;
  while (true) {
    auto block = block_mgr_.GetBlock(client_rank, len);
    if (len == -1) {
      break;
    }
    msgpack::pack(&sbuf, msgpack::type::raw_ref(block.get(), len));
    refs.push_back(std::move(block));
  }
}

void PairwiseShuffleServer::UnpackBlocks(const char *buf, size_t len) {
  size_t offset = 0;
  msgpack::unpacked unpacked;
  while (offset != len) {
    msgpack::unpack(&unpacked, buf, len, &offset);
    auto raw = unpacked.get().via.raw;
    std::unique_ptr<char[]> block(new char[raw.size]);
    memcpy(block.get(), raw.ptr, raw.size);
    block_mgr_.PutBlock(my_rank_, raw.size, std::move(block));
  }
}
