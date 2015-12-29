//
// Created by Harunobu Daikoku on 2015/10/29.
//

#include <sstream>
#include <fstream>
#include "executor.h"
#include "key_value_rdd.h"
#include "key_values_rdd.h"

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

    } else if (method == "map_with_combine") {
      // Map with in-mapper combining
      req.result(MapWithCombine(req));

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

  ParseParams(req, id_, executors_);
  std::cout << "my executor_id: " << id_ << std::endl;

  return rdd_rpc::Response::OK;
}

rdd_rpc::Response Executor::DistributeText(msgpack::rpc::request &req) {
  std::cout << "distribute_text called" << std::endl;

  int rdd_id;
  std::string filename;
  std::vector<std::pair<long long int, int>> indices;

  ParseParams(req, rdd_id, filename, indices);

  for (const auto &index : indices) {
    rdds_[rdd_id].push_back(
        std::unique_ptr<KeyValueRDD<long long int, std::string>>
            (new KeyValueRDD<long long int, std::string>(filename, index.first, index.second)));
  }

  return rdd_rpc::Response::OK;
}

rdd_rpc::Response Executor::Map(msgpack::rpc::request &req) {
  std::cout << "map called" << std::endl;

  int rdd_id, new_rdd_id;
  std::string dl_filename;
  ParseParams(req, rdd_id, dl_filename, new_rdd_id);

  auto &rdds = rdds_[rdd_id];
  auto &new_rdds = rdds_[new_rdd_id];
  tbb::parallel_for(
      tbb::blocked_range<int>(0, rdds.size(), 1),
      [&rdds, &new_rdds, &dl_filename](tbb::blocked_range<int> &range) {
        for (int i = range.begin(); i < range.end(); i++) {
          // TODO dirty hack :)
          new_rdds.push_back(static_cast<KeyValueRDD<long long int, std::string> *>(rdds[i].get())
                                 ->Map<std::pair<std::string, std::string>, int>(dl_filename));
        }
      }
  );

  return rdd_rpc::Response::OK;
}

rdd_rpc::Response Executor::MapWithCombine(msgpack::rpc::request &req) {
  std::cout << "map with combine called" << std::endl;

  int rdd_id, new_rdd_id;
  std::string dl_mapper, dl_combiner;
  ParseParams(req, rdd_id, dl_mapper, dl_combiner, new_rdd_id);

  std::vector<std::pair<std::string, std::string>> executors;

  for (int i = 0; i < executors_.size(); ++i) {
    if (i == id_) {
      continue;
    }
    executors.push_back(std::make_pair(executors_[i].first, "60090"));
  }

  ShuffleServer shuffle_server(std::to_string(data_port_), block_mgr_);
  ShuffleClient shuffle_client(executors, id_, block_mgr_);
  auto server_thread = shuffle_server.Start();
  auto client_thread = shuffle_client.Start();

  auto &rdds = rdds_[rdd_id];
  auto &new_rdds = rdds_[new_rdd_id];
  tbb::parallel_for(
      tbb::blocked_range<int>(0, rdds.size(), 1),
      [&](tbb::blocked_range<int> &range) {
        for (int i = range.begin(); i < range.end(); i++) {
          // TODO dirty hack :)
          auto new_rdd = static_cast<KeyValueRDD<long long int, std::string> *>(rdds[i].get())
              ->Map<std::pair<std::string, std::string>, int>(dl_mapper);
          new_rdd->Combine(dl_combiner);
          new_rdd->PutBlocks(block_mgr_);
          new_rdds.push_back(std::move(new_rdd));
        }
      }
  );
  block_mgr_.Finalize();

  server_thread.join();
  client_thread.join();

  return rdd_rpc::Response::OK;
}

//rdd_rpc::Response Executor::Combine(msgpack::rpc::request &req) {
//  std::cout << "combine called" << std::endl;
//
//  int rdd_id;
//  std::string dl_filename;
//  ParseParams(req, rdd_id, dl_filename);
//
//  auto &rdds = rdds_[rdd_id];
//  tbb::parallel_for(
//      tbb::blocked_range<int>(0, rdds.size(), 1),
//      [&rdds, &dl_filename](tbb::blocked_range<int> &range) {
//        for (int i = range.begin(); i < range.end(); i++) {
//          // TODO dirty hack :)
//          static_cast<KeyValuesRDD<std::pair<std::string, std::string>, int> *>(rdds[i].get())
//              ->Combine(dl_filename);
//        }
//      }
//  );
//
//  return rdd_rpc::Response::OK;
//}

rdd_rpc::Response Executor::ShuffleSrv(msgpack::rpc::request &req) {
  std::cout << "shuffle_srv called" << std::endl;

  int rdd_id, dest_id, n_reducers;
  ParseParams(req, rdd_id, dest_id, n_reducers);

  if (rdds_[rdd_id].size() > 1) {
    int i = 0;
    for (const auto &rdd : rdds_[rdd_id]) {
      if (i++ == 0) continue;
      // TODO dirty hack :)
      //static_cast<KeyValuesRDD<std::pair<std::string, std::string>, int> *>(rdd.get())->MergeTo(
      //    static_cast<KeyValuesRDD<std::pair<std::string, std::string>, int> *>(rdds_[rdd_id][0].get()));
    }
  }

  auto rdd = std::move(rdds_[rdd_id][0]);
  rdds_[rdd_id].clear();
  rdds_[rdd_id].push_back(std::move(rdd));

  // TODO dirty hack :)
  //if (!static_cast<KeyValuesRDD<std::pair<std::string, std::string>, int> *>(rdds_[rdd_id][0].get())
  //    ->ShuffleServer(dest_id, n_reducers, data_port_)) {
  //  return rdd_rpc::Response::ERR;
  //}

  return rdd_rpc::Response::OK;
}

rdd_rpc::Response Executor::ShuffleCli(msgpack::rpc::request &req) {
  std::cout << "shuffle_cli called" << std::endl;

  std::string dest;
  int rdd_id, dest_id, n_reducers;
  ParseParams(req, rdd_id, dest, dest_id, n_reducers);

  if (rdds_[rdd_id].size() > 1) {
    int i = 0;
    for (const auto &rdd : rdds_[rdd_id]) {
      if (i++ == 0) continue;
      // TODO dirty hack :)
      //static_cast<KeyValuesRDD<std::pair<std::string, std::string>, int> *>(rdd.get())->MergeTo(
      //    static_cast<KeyValuesRDD<std::pair<std::string, std::string>, int> *>(rdds_[rdd_id][0].get()));
    }
  }

  auto rdd = std::move(rdds_[rdd_id][0]);
  rdds_[rdd_id].clear();
  rdds_[rdd_id].push_back(std::move(rdd));

  // TODO dirty hack :)
  //if (!static_cast<KeyValuesRDD<std::pair<std::string, std::string>, int> *>(rdds_[rdd_id][0].get())
  //    ->ShuffleClient(dest, dest_id, n_reducers)) {
  //  return rdd_rpc::Response::ERR;
  //}

  return rdd_rpc::Response::OK;
}

rdd_rpc::Response Executor::Reduce(msgpack::rpc::request &req) {
  std::cout << "reduce called" << std::endl;

  int rdd_id, new_rdd_id;
  std::string dl_filename;
  ParseParams(req, rdd_id, dl_filename, new_rdd_id);

  auto kvs_rdd = static_cast<KeyValuesRDD<std::pair<std::string, std::string>, int> *>(rdds_[rdd_id][0].get());
  kvs_rdd->GetBlocks(block_mgr_, id_);

  // TODO dirty hack :)
  rdds_[new_rdd_id].push_back(kvs_rdd->Reduce<std::pair<std::string, std::string>, int>(dl_filename));

  return rdd_rpc::Response::OK;
}

rdd_rpc::Response Executor::Print(msgpack::rpc::request &req) {
  std::cout << "print called" << std::endl;

  int rdd_id;
  ParseParams(req, rdd_id);

  rdds_[rdd_id][0]->Print();

  return rdd_rpc::Response::OK;
}
