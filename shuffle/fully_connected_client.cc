//
// Created by Harunobu Daikoku on 2016/04/15.
//

#include <unordered_map>
#include <unordered_set>
#include "shuffle/fully_connected_client.h"

void FullyConnectedClient::Run() {
  // make connections to all the servers
  for (const auto &client : clients_) {
    if (client->Connect() < 0) {
      std::cerr << "CLIENT: could not connect to one or more servers" << std::endl;
      return;
    }
  }

  int backoff = kMinBackoff;
  std::unordered_map<int, std::unordered_set<int>> no_more_blocks;

  while (partition_ids_.size() > 0) {
    auto p = partition_ids_.front();
    partition_ids_.pop();

    auto &no_more = no_more_blocks[p];

    std::this_thread::sleep_for(std::chrono::milliseconds(backoff));
    if (backoff < kMaxBackoff) {
      backoff *= 2;
    }

    bool remove_partition = true;
    for (auto &client : clients_) {
      if (no_more.find(client->GetSockFd()) != no_more.end()) {
        continue;
      }
      client->Write(&p, sizeof(p));

      int32_t len;
      if (client->Read(&len, sizeof(len)) < 0) {
        std::cerr << "CLIENT: could not read from the server" << std::endl;
        break;
      }
      if (len < 0) {
        // there's no more blocks to fetch
        no_more.insert(client->GetSockFd());
        continue;
      }

      remove_partition = false;
      if (len > 0) {
        backoff = kMinBackoff;
        std::unique_ptr<char[]> block(new char[len]);
        client->Read(block.get(), len);
        block_mgr_.PutBlock(p, len, std::move(block));
      }
    }

    if (!remove_partition) {
      partition_ids_.push(p);
    }
  }

  // close connections
  for (auto &client : clients_) {
    client.reset();
  }
}

