//
// Created by Harunobu Daikoku on 2015/10/29.
//

#include "worker/executor.h"

#include "text_file_index.h"
#include "worker/text_file_rdd.h"
#include "worker/key_values_rdd.h"
#include "worker/shuffle/fully_connected_client.h"
#include "worker/shuffle/fully_connected_server.h"
#include "worker/shuffle/pairwise_shuffle_server.h"
#include "worker/shuffle/pairwise_shuffle_client.h"

void Executor::dispatch(msgpack::rpc::request req) {
  std::string method;

  req.method().convert(&method);
  try {
    if (method == "hello") {
      // initial greeting
      req.result(Hello(req));

    } else if (method == "textfile") {
      // create TextFileRDD from received indices of a text file
      req.result(TextFile(req));

    } else if (method == "map") {
      // Map specified RDD
      req.result(Map(req));

    } else if (method == "map_with_shuffle") {
      // Map overlapped by shuffle
      req.result(MapWithShuffle(req));

    } else if (method == "shuffle_srv") {
      // shuffle, act as a server
      req.result(ShuffleSrv(req));

    } else if (method == "shuffle_cli") {
      // shuffle, act as a client
      req.result(ShuffleCli(req));

    } else if (method == "reduce") {
      // reduce
      req.result(Reduce(req));

    } else if (method == "group_by") {
      // groupBy
      req.result(GroupBy(req));

    } else if (method == "print") {
      // print key-values
      req.result(Print(req));

    } else if (method == "clear") {
      // clear rdd
      req.result(Clear(req));

    } else {
      // there is no such methods
      req.error(msgpack::rpc::NO_METHOD_ERROR);
    }

  } catch (msgpack::type_error &e) {
    std::cerr << "ERROR: type_error" << std::endl;
    req.error(msgpack::rpc::ARGUMENT_ERROR);
  }
}


rdd_rpc::Response Executor::Hello(msgpack::rpc::request &req) {
  ParseParams(req, my_executor_id_, executors_);
  std::cout << "my executor_id: " << my_executor_id_ << std::endl;

  return rdd_rpc::Response::OK;
}

rdd_rpc::Response Executor::TextFile(msgpack::rpc::request &req) {
  int rdd_id;
  int num_partitions;
  std::string filename;
  std::vector<TextFileIndex> indices;

  ParseParams(req, rdd_id, num_partitions, filename, indices);

  block_mgr_.reset(new BlockManager(num_partitions));

  for (const auto &index : indices) {
    rdds_[rdd_id].push_back(
        std::unique_ptr<TextFileRDD>(new TextFileRDD(num_partitions, filename, index))
    );
  }

  return rdd_rpc::Response::OK;
}

rdd_rpc::Response Executor::Map(msgpack::rpc::request &req) {
  std::cout << "map with combine called" << std::endl;

  int rdd_id, new_rdd_id;
  std::string dl_mapper, dl_combiner;
  ParseParams(req, rdd_id, dl_mapper, dl_combiner, new_rdd_id);

  auto &rdds = rdds_[rdd_id];
  auto &new_rdds = rdds_[new_rdd_id];
  tbb::parallel_for_each(rdds.begin(), rdds.end(), [&](const std::unique_ptr<RDD> &rdd) {
    // TODO dirty hack :)
    auto mapped = static_cast<TextFileRDD *>(rdd.get())
        ->Map<std::string, int>(dl_mapper);
    if (dl_combiner != "") {
      mapped->Combine(dl_combiner);
    }
    mapped->PutBlocks(*block_mgr_);
    new_rdds.push_back(std::move(mapped));
  });

  block_mgr_->Finalize();

  return rdd_rpc::Response::OK;
}

rdd_rpc::Response Executor::MapWithShuffle(msgpack::rpc::request &req) {
  std::cout << "map with combine/shuffle called" << std::endl;

  int rdd_id, new_rdd_id;
  std::string dl_mapper, dl_combiner;
  std::unordered_map<int, std::vector<int>> partitions_by_owner;
  ParseParams(req, rdd_id, dl_mapper, dl_combiner, partitions_by_owner, new_rdd_id);

  std::vector<std::pair<std::string, int>> executors;
  partitions_by_owner.erase(my_executor_id_);
  for (const auto &p : partitions_by_owner) {
    auto &owner_id = p.first;
    executors.push_back(std::make_pair(executors_[owner_id].GetAddr(), executors_[owner_id].GetDataPort()));
  }

  FullyConnectedServer shuffle_server(executors_[my_executor_id_].GetDataPort(), *block_mgr_, partitions_by_owner);
  auto server_thread = shuffle_server.Dispatch();
  FullyConnectedClient shuffle_client(executors, my_executor_id_, *block_mgr_);
  auto client_thread = shuffle_client.Dispatch();

  auto &rdds = rdds_[rdd_id];
  auto &new_rdds = rdds_[new_rdd_id];
  tbb::parallel_for_each(rdds.begin(), rdds.end(), [&](const std::unique_ptr<RDD> &rdd) {
    // TODO dirty hack :)
    auto mapped = static_cast<TextFileRDD *>(rdd.get())
        ->Map<std::string, int>(dl_mapper);
    if (dl_combiner != "") {
      mapped->Combine(dl_combiner);
    }
    mapped->PutBlocks(*block_mgr_);
    new_rdds.push_back(std::move(mapped));
  });

  block_mgr_->Finalize();

  client_thread.join();
  server_thread.join();

  return rdd_rpc::Response::OK;
}

rdd_rpc::Response Executor::ShuffleSrv(msgpack::rpc::request &req) {
  std::vector<int> partition_ids;
  ParseParams(req, partition_ids);

  PairwiseShuffleServer shuffle_server(my_executor_id_, *block_mgr_);
  shuffle_server.Start(partition_ids, executors_[my_executor_id_].GetDataPort());

  return rdd_rpc::Response::OK;
}

rdd_rpc::Response Executor::ShuffleCli(msgpack::rpc::request &req) {
  std::vector<int> partition_ids;
  std::string server_addr;
  int server_port;
  ParseParams(req, partition_ids, server_addr, server_port);

  PairwiseShuffleClient shuffle_client(my_executor_id_, *block_mgr_);
  shuffle_client.Start(partition_ids, server_addr, server_port);

  return rdd_rpc::Response::OK;
}

rdd_rpc::Response Executor::Reduce(msgpack::rpc::request &req) {
  int rdd_id, new_rdd_id;
  std::string dl_reducer;
  ParseParams(req, rdd_id, dl_reducer, new_rdd_id);

  auto &rdds = rdds_[rdd_id];
  auto &new_rdds = rdds_[new_rdd_id];
  tbb::parallel_for_each(rdds.begin(), rdds.end(), [&](const std::unique_ptr<RDD> &rdd) {
    auto kvs_rdd = static_cast<KeyValuesRDD<std::string, int> *>(rdd.get());
    kvs_rdd->GetBlocks(*block_mgr_);
    new_rdds.push_back(kvs_rdd->Reduce(dl_reducer));
  });

  return rdd_rpc::Response::OK;
}

rdd_rpc::Response Executor::GroupBy(msgpack::rpc::request &req) {
  int rdd_id;
  ParseParams(req, rdd_id);

  auto &rdds = rdds_[rdd_id];
  tbb::parallel_for_each(rdds.begin(), rdds.end(), [&](const std::unique_ptr<RDD> &rdd) {
    rdd->GetBlocks(*block_mgr_);
  });

  return rdd_rpc::Response::OK;
}

rdd_rpc::Response Executor::Print(msgpack::rpc::request &req) {
  int rdd_id;
  ParseParams(req, rdd_id);

  auto &rdds = rdds_[rdd_id];
  for (const auto &rdd : rdds) {
    rdd->Print();
  }

  return rdd_rpc::Response::OK;
}

rdd_rpc::Response Executor::Clear(msgpack::rpc::request &req) {
  int rdd_id;
  ParseParams(req, rdd_id);

  rdds_.erase(rdd_id);

  return rdd_rpc::Response::OK;
}
