//
// Created by Harunobu Daikoku on 2016/01/06.
//

#include "slave/rpc_shuffle_server.h"

void RPCShuffleServer::dispatch(msgpack::rpc::request req) {
  try {
    std::string method;
    req.method().convert(&method);
    if (method == "GET") {
      msgpack::type::tuple<int> params;
      req.params().convert(&params);

      int client_id = params.get<0>();
      long block_len = 0;
      auto block = block_mgr_.GetBlock(client_id, block_len);
      std::string header(std::to_string(block_len) + "\r\n");

      if (block_len > 0) {
        std::string body(block.get(), block_len);
        req.result(header.append(body));
      } else {
        req.result(header);
        if (block_len == -1)
          n_finished_++;
      }

    } else {
      req.error(msgpack::rpc::NO_METHOD_ERROR);
    }
  } catch (msgpack::type_error &e) {
    req.error(msgpack::rpc::ARGUMENT_ERROR);
  } catch (std::exception &e) {
    // TODO:
  }

  if (n_finished_ == n_clients_) {
    this->instance.end();
  }
}
