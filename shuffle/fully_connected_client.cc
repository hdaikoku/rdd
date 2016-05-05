//
// Created by Harunobu Daikoku on 2016/04/15.
//

#include "shuffle/fully_connected_client.h"

void FullyConnectedClient::Run() {
// make connections to all the servers
  for (const auto &client : clients_) {
    if (client->Connect() < 0) {
      std::cerr << "CLIENT: could not connect to one or more servers" << std::endl;
      return;
    }
  }

  // fetch blocks
  int backoff = kMinBackoff;
  while (clients_.size() > 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(backoff));
    if (backoff < kMaxBackoff) {
      backoff *= 2;
    }

    for (auto &client : clients_) {
      bool close_conn = true;
      for (auto &p : partition_ids_) {
        client->Write(&p, sizeof(p));

        int32_t len;
        if (client->Read(&len, sizeof(len)) < 0) {
          std::cerr << "CLIENT: could not read from the server" << std::endl;
          break;
        }
        if (len < 0) {
          continue;
        }

        close_conn = false;
        if (len > 0) {
          backoff = kMinBackoff;
          std::unique_ptr<char[]> block(new char[len]);
          client->Read(block.get(), len);
          block_mgr_.PutBlock(p, len, std::move(block));
        }
      }

      if (close_conn) {
        client.reset();
      }
    }
    // clear clients from which all data has been received
    clients_.erase(
        std::remove_if(clients_.begin(), clients_.end(),
                       [](const std::unique_ptr<SocketClient> &client) {
                         return !client;
                       }),
        clients_.end()
    );
  }
}

