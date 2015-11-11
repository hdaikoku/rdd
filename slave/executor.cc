//
// Created by Harunobu Daikoku on 2015/10/29.
//

#include "executor.h"
#include "rdd_rpc.h"
#include "text_rdd.h"

void Executor::dispatch(msgpack::rpc::request req) {
  std::string method;

  req.method().convert(&method);

  try {
    if (method == "hello") {
      msgpack::type::tuple<int> id;

      req.params().convert(&id);
      SetExecutorId(id.get<0>());

      req.result(std::string("ok"));

    } else if (method == "distribute") {
      std::cout << "distribute called" << std::endl;

      msgpack::type::tuple<int, std::string> params;
      req.params().convert(&params);

      std::cout << "received: " << params.get<1>().length() << std::endl;

      CreateTextRdd(params.get<0>(), params.get<1>());

      req.result(rdd_rpc::Response::OK);
    } else if (method == "map") {
      std::cout << "map called" << std::endl;

      msgpack::type::tuple<int, std::string, int> params;
      req.params().convert(&params);

      int rdd_id = params.get<0>();
      std::string dl_filename = params.get<1>();
      int new_rdd_id = params.get<2>();

      for (auto &rdd : rdds_[rdd_id]) {
        rdds_[new_rdd_id].push_back(
            static_cast<TextRdd *>(rdd.get())->Map<std::string, int>(dl_filename)
        );
      }

      req.result(rdd_rpc::Response::OK);

    } else {
      req.error(msgpack::rpc::NO_METHOD_ERROR);
    }
  } catch (msgpack::type_error &e) {
    std::cerr << "ERROR: " << std::endl;
    req.error(msgpack::rpc::ARGUMENT_ERROR);
  }
}


void Executor::SetExecutorId(int id) {
  id_ = id;
  std::cout << "my executor_id: " << id_ << std::endl;
}

void Executor::CreateTextRdd(const int rdd_id, const std::string &data) {
  rdds_[rdd_id].push_back(std::unique_ptr<TextRdd>(new TextRdd(data)));
}
