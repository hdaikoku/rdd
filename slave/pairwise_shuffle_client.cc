//
// Created by Harunobu Daikoku on 2016/01/11.
//

#include <iostream>
#include "socket/socket_client.h"
#include "slave/pairwise_shuffle_client.h"


void PairwiseShuffleClient::Start(int server_id, const std::string &server_addr, int server_port) {
  int sock_fd;
  SocketClient client(server_addr, std::to_string(server_port));
  std::cout << "connecting to " << server_addr << ":" << server_port << std::endl;

  if ((sock_fd = client.Connect()) < 0) {
    std::cerr << "could not connect to: " << server_addr << ":" << server_port << std::endl;
    return;
  }

  msgpack::sbuffer sbuf;
  std::vector<std::unique_ptr<char[]>> refs;
  PackBlocks(server_id, sbuf, refs);

  if (client.WriteWithHeader(sock_fd, sbuf.data(), sbuf.size()) < 0) {
    std::cerr << "write failed" << std::endl;
    return;
  }
  free(sbuf.release());

  size_t len = 0;
  auto rbuf = client.ReadWithHeader(sock_fd, len);
  if (!rbuf) {
    std::cerr << "read failed" << std::endl;
    return;
  }

  UnpackBlocks(rbuf.get(), len);
}

void PairwiseShuffleClient::PackBlocks(int server_rank,
                                       msgpack::sbuffer &sbuf,
                                       std::vector<std::unique_ptr<char[]>> &refs) {
  long len = 0;
  while (true) {
    auto block = block_mgr_.GetBlock(server_rank, len);
    if (len == -1) {
      break;
    }
    msgpack::pack(&sbuf, msgpack::type::raw_ref(block.get(), len));
    refs.push_back(std::move(block));
  }
}

void PairwiseShuffleClient::UnpackBlocks(const char *buf, size_t len) {
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
