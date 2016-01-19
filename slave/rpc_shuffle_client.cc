//
// Created by Harunobu Daikoku on 2016/01/06.
//

#include <queue>
#include "rpc_shuffle_client.h"

bool RPCShuffleClient::FetchBlocks() {
  std::queue<msgpack::rpc::future> futures;
  const int kMinBackoff = 1;
  const int kMaxBackoff = 1000;
  int backoff = kMinBackoff;

  while (!servers_.empty()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(backoff));
    if (backoff < kMaxBackoff) {
      backoff *= 2;
    }

    for (const auto server : servers_) {
      futures.push(sp_.get_session(server.first, server.second)
                       .call("GET", my_rank_));
    }

    while (!futures.empty()) {
      auto f = futures.front();
      futures.pop();
      auto s = servers_.front();
      servers_.pop_front();
      try {
        auto result = f.get<std::string>();
        auto header_end = result.find("\r\n");
        auto len = std::stol(result.substr(0, header_end));
        switch (len) {
          case -1:
            continue;
          case 0:
            break;
          default:
            std::unique_ptr<char[]> block(new char[len]);
            result.copy(block.get(), len, header_end + 2);
            block_mgr_.PutBlock(my_rank_, len, std::move(block));
            backoff = kMinBackoff;
            break;
        }
      } catch (msgpack::rpc::remote_error &e) {
        std::cerr << e.what() << std::endl;
      }
      servers_.push_back(s);
    }
  }

  return true;
}
