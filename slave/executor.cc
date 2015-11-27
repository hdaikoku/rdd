//
// Created by Harunobu Daikoku on 2015/10/29.
//

#include <sstream>
#include "slave/executor.h"
#include "slave/key_value_rdd.h"
#include "slave/key_values_rdd.h"

void Executor::dispatch(msgpack::rpc::request req) {
  std::string method;

  req.method().convert(&method);
  try {
    if (method == "hello") {
      // initial greeting
      req.result(Hello(req));

    } else if (method == "distribute") {
      // create KeyValueRDD from received text
      req.result(DistributeText(req));
      
    } else if (method == "map") {
      // Map specified RDD
      req.result(Map(req));

    } else if (method == "shuffle_srv") {
      // shuffle, act as a server
      req.result(ShuffleSrv(req));

    } else if (method == "shuffle_cli") {
      // shuffle, act as a client
      req.result(ShuffleCli(req));

    } else if (method == "reduce") {
      // reduce
      req.result(Reduce(req));

    } else if (method == "print") {
      // print key-values
      req.result(Print(req));

    } else {
      // there is no such methods
      req.error(msgpack::rpc::NO_METHOD_ERROR);
    }

  } catch (msgpack::type_error &e) {
    std::cerr << "ERROR: " << std::endl;
    req.error(msgpack::rpc::ARGUMENT_ERROR);
  }
}


rdd_rpc::Response Executor::Hello(msgpack::rpc::request &req) {
  std::cout << "hello called" << std::endl;
  int id;

  ParseParams(req, id);
  SetExecutorId(id);

  return rdd_rpc::Response::OK;
}

rdd_rpc::Response Executor::DistributeText(msgpack::rpc::request &req) {
  std::cout << "distribute_text called" << std::endl;

  int rdd_id, offset;
  std::string text, line;

  ParseParams(req, rdd_id, text, offset);

  if (rdds_.find(rdd_id) == rdds_.end()) {
    rdds_[rdd_id] = std::unique_ptr<KeyValueRDD<int, std::string>>(new KeyValueRDD<int, std::string>());
  }

  std::istringstream stream(text);
  while (std::getline(stream, line)) {
    int pos = offset + stream.tellg();
    static_cast<KeyValueRDD<int, std::string> *>(rdds_[rdd_id].get())
        ->Insert(pos, line);
  }

  std::cout << "received: " << text.length() << " bytes" << std::endl;
  return rdd_rpc::Response::OK;
}

rdd_rpc::Response Executor::Map(msgpack::rpc::request &req) {
  std::cout << "map called" << std::endl;

  int rdd_id, new_rdd_id;
  std::string dl_filename;
  ParseParams(req, rdd_id, dl_filename, new_rdd_id);

  rdds_[new_rdd_id] =
      static_cast<KeyValueRDD<int, std::string> *>(rdds_[rdd_id].get())->Map<std::string, int>(dl_filename);

  return rdd_rpc::Response::OK;
}

rdd_rpc::Response Executor::ShuffleSrv(msgpack::rpc::request &req) {
  std::cout << "shuffle_srv called" << std::endl;

  int rdd_id, dest_id, n_reducers;
  ParseParams(req, rdd_id, dest_id, n_reducers);

  // TODO dirty hack :)
  if (!static_cast<KeyValuesRDD<std::string, int> *>(rdds_[rdd_id].get())->ShuffleServer(dest_id,
                                                                                         n_reducers,
                                                                                         data_port_)) {
    return rdd_rpc::Response::ERR;
  }

  return rdd_rpc::Response::OK;
}

rdd_rpc::Response Executor::ShuffleCli(msgpack::rpc::request &req) {
  std::cout << "shuffle_cli called" << std::endl;

  std::string dest;
  int rdd_id, dest_id, n_reducers;
  ParseParams(req, rdd_id, dest, dest_id, n_reducers);

  // TODO dirty hack :)
  if (!static_cast<KeyValuesRDD<std::string, int> *>(rdds_[rdd_id].get())->ShuffleClient(dest, dest_id, n_reducers)) {
    return rdd_rpc::Response::ERR;
  }

  return rdd_rpc::Response::OK;
}

rdd_rpc::Response Executor::Reduce(msgpack::rpc::request &req) {
  std::cout << "reduce called" << std::endl;

  int rdd_id, new_rdd_id;
  std::string dl_filename;
  ParseParams(req, rdd_id, dl_filename, new_rdd_id);

  // TODO dirty hack :)
  rdds_[new_rdd_id] =
      static_cast<KeyValuesRDD<std::string, int> *>(rdds_[rdd_id].get())->Reduce<std::string, int>(dl_filename);

  return rdd_rpc::Response::OK;
}

rdd_rpc::Response Executor::Print(msgpack::rpc::request &req) {
  std::cout << "print called" << std::endl;

  int rdd_id;
  ParseParams(req, rdd_id);

  rdds_[rdd_id]->Print();

  return rdd_rpc::Response::OK;
}

void Executor::SetExecutorId(int id) {
  id_ = id;
  std::cout << "my executor_id: " << id_ << std::endl;
}
