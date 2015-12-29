//
// Created by Harunobu Daikoku on 2015/12/24.
//

#include <iostream>
#include "shuffle_client.h"

bool ShuffleClient::FetchBlocks() {
  char header[16];

  while (!clients_queue_.empty()) {
    auto client = clients_queue_.front();
    clients_queue_.pop();

    auto sock_fd = client.Connect();
    client.Write(sock_fd, &my_rank_, sizeof(my_rank_));
    client.Read(sock_fd, header, 16);
    auto end = std::string(header).find_first_of("\r\n");
    header[end] = '\0';

    auto len = std::stol(header);
    if (len == -1) {
      continue;
    }
    if (len > 0) {
      //std::cout << "receiving " << len << "bytes..." << std::endl;
      std::unique_ptr<char[]> buf(new char[len]);
      client.Read(sock_fd, buf.get(), len);
      block_mgr_.PutBlock(my_rank_, len, std::move(buf));
    } else {
      sleep(1);
    }
    clients_queue_.push(client);
  }

  return true;
}

