//
// Created by Harunobu Daikoku on 2016/01/06.
//

#include <queue>
#include "rpc_shuffle_client.h"

bool RPCShuffleClient::FetchBlocks() {
  int n_servers = servers_.size();
  int finished = 0;
  std::queue<msgpack::rpc::future> futures;
  while (finished < n_servers) {
    for (const auto server : servers_) {
      futures.push(sp_.get_session(server.first, server.second)
                       .call("GET", my_rank_));
    }

    while (!futures.empty()) {
      auto f = futures.front();
      futures.pop();
      try {
        auto result = f.get<std::string>();
        auto header_end = result.find_first_of("\r\n");
        auto len = std::stol(result.substr(0, header_end));
        switch (len) {
          case -1:
            finished++;
          case 0:
            break;
          default:
            std::unique_ptr<char[]> block(new char[len]);
            result.copy(block.get(), len, header_end + 2);
            block_mgr_.PutBlock(my_rank_, len, std::move(block));
            break;
        }
      } catch (msgpack::rpc::remote_error &e) {
        std::cerr << e.what() << std::endl;
      }
    }
    sleep(1);
  }

  return true;
}

