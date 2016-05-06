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

  int32_t len;
  int backoff = kMinBackoff;
  std::vector<char> rbuf(1 << 19);
  while (clients_.size() > 0) {
    for (auto &client : clients_) {
      client->Write(&my_owner_id_, sizeof(my_owner_id_));

      if (client->Read(&len, sizeof(len)) < 0) {
        std::cerr << "CLIENT: could not read from the server" << std::endl;
        break;
      }

      if (len < 0) {
        // there's no more blocks to fetch
        client.reset();
      } else if (len > 0) {
        backoff = kMinBackoff;
        if (rbuf.capacity() < len) {
          rbuf.resize(len);
        }
        client->Read(rbuf.data(), len);
        block_mgr_.GroupUnpackBlocks(rbuf.data(), len);
      }
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(backoff));
    if (backoff < kMaxBackoff) {
      backoff *= 2;
    }

    clients_.erase(
        std::remove_if(clients_.begin(), clients_.end(),
                       [](const std::unique_ptr<SocketClient> &client) {
                         return !client;
                       }),
        clients_.end()
    );
  }
}

