//
// Created by Harunobu Daikoku on 2015/10/29.
//

#include <sstream>
#include <fstream>
#include "shuffle/fully_connected_client.h"
#include "shuffle/fully_connected_server.h"
#include "text_file_index.h"
#include "slave/text_file_rdd.h"
#include "shuffle/pairwise_shuffle_server.h"
#include "shuffle/pairwise_shuffle_client.h"
#include "slave/executor.h"
#include "slave/key_values_rdd.h"

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

    } else if (method == "map_with_combine") {
      // Map with in-mapper combining
      req.result(MapWithCombine(req));

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
  std::cout << "hello called" << std::endl;

  ParseParams(req, id_, executors_);
  std::cout << "my executor_id: " << id_ << std::endl;

  return rdd_rpc::Response::OK;
}

rdd_rpc::Response Executor::TextFile(msgpack::rpc::request &req) {
  std::cout << "textfile called" << std::endl;

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
  std::cout << "map called" << std::endl;

  int rdd_id, new_rdd_id;
  std::string dl_mapper;
  ParseParams(req, rdd_id, dl_mapper, new_rdd_id);

  auto &rdds = rdds_[rdd_id];
  auto &new_rdds = rdds_[new_rdd_id];
  tbb::parallel_for(
      tbb::blocked_range<int>(0, rdds.size(), 1),
      [&](tbb::blocked_range<int> &range) {
        for (int i = range.begin(); i < range.end(); i++) {
          // TODO dirty hack :)
          auto new_rdd = static_cast<TextFileRDD *>(rdds[i].get())
              ->Map<std::string, int>(dl_mapper);
          new_rdd->PutBlocks(*block_mgr_);
          new_rdds.push_back(std::move(new_rdd));
        }
      }
  );
  block_mgr_->Finalize();

  return rdd_rpc::Response::OK;
}

rdd_rpc::Response Executor::MapWithCombine(msgpack::rpc::request &req) {
  std::cout << "map with combine called" << std::endl;

  int rdd_id, new_rdd_id;
  std::string dl_mapper, dl_combiner;
  ParseParams(req, rdd_id, dl_mapper, dl_combiner, new_rdd_id);

  auto &rdds = rdds_[rdd_id];
  assert(rdds.size() > 0);

  auto &new_rdds = rdds_[new_rdd_id];
  tbb::parallel_for(
      tbb::blocked_range<int>(0, rdds.size(), 1),
      [&](tbb::blocked_range<int> &range) {
        for (int i = range.begin(); i < range.end(); i++) {
          // TODO dirty hack :)
          auto new_rdd = static_cast<TextFileRDD *>(rdds[i].get())
              ->Map<std::string, int>(dl_mapper);
          new_rdd->Combine(dl_combiner);
          new_rdd->PutBlocks(*block_mgr_);
          new_rdds.push_back(std::move(new_rdd));
        }
      }
  );
  block_mgr_->Finalize();

  return rdd_rpc::Response::OK;
}

rdd_rpc::Response Executor::MapWithShuffle(msgpack::rpc::request &req) {
  std::cout << "map with shuffle called" << std::endl;

  int rdd_id, new_rdd_id;
  std::string dl_mapper, dl_combiner;
  std::vector<int> owner_ids;
  std::vector<int> partition_ids;
  ParseParams(req, rdd_id, dl_mapper, dl_combiner, owner_ids, partition_ids, new_rdd_id);

  std::vector<std::pair<std::string, int>> executors;
  for (const auto &i : owner_ids) {
    if (i != id_) {
      executors.push_back(std::make_pair(executors_[i].GetAddr(), executors_[i].GetDataPort()));
    }
  }

  int num_clients = owner_ids.size() - 1;
  FullyConnectedServer shuffle_server(executors_[id_].GetDataPort(), *block_mgr_, num_clients);
  auto server_thread = shuffle_server.Dispatch();
  FullyConnectedClient shuffle_client(executors, partition_ids, *block_mgr_);
  auto client_thread = shuffle_client.Dispatch();

  auto &rdds = rdds_[rdd_id];
  auto &new_rdds = rdds_[new_rdd_id];
  tbb::parallel_for(
      tbb::blocked_range<int>(0, rdds.size(), 1),
      [&](tbb::blocked_range<int> &range) {
        for (int i = range.begin(); i < range.end(); i++) {
          // TODO dirty hack :)
          auto new_rdd = static_cast<TextFileRDD *>(rdds[i].get())
              ->Map<std::string, int>(dl_mapper);
          new_rdd->Combine(dl_combiner);
          new_rdd->PutBlocks(*block_mgr_);
          new_rdds.push_back(std::move(new_rdd));
        }
      }
  );
  block_mgr_->Finalize();

  client_thread.join();
  server_thread.join();

  return rdd_rpc::Response::OK;
}

rdd_rpc::Response Executor::ShuffleSrv(msgpack::rpc::request &req) {
  std::cout << "shuffle_srv called" << std::endl;

  std::vector<int> partition_ids;
  ParseParams(req, partition_ids);

  PairwiseShuffleServer shuffle_server(id_, *block_mgr_);
  shuffle_server.Start(partition_ids, executors_[id_].GetDataPort());

  return rdd_rpc::Response::OK;
}

rdd_rpc::Response Executor::ShuffleCli(msgpack::rpc::request &req) {
  std::cout << "shuffle_cli called" << std::endl;

  std::vector<int> partition_ids;
  std::string server_addr;
  int server_port;
  ParseParams(req, partition_ids, server_addr, server_port);

  PairwiseShuffleClient shuffle_client(id_, *block_mgr_);
  shuffle_client.Start(partition_ids, server_addr, server_port);

  return rdd_rpc::Response::OK;
}

rdd_rpc::Response Executor::Reduce(msgpack::rpc::request &req) {
  std::cout << "reduce called" << std::endl;

  int rdd_id, new_rdd_id;
  std::string dl_filename;
  ParseParams(req, rdd_id, dl_filename, new_rdd_id);

  auto &rdds = rdds_[rdd_id];
  auto &new_rdds = rdds_[new_rdd_id];
  tbb::parallel_for(
      tbb::blocked_range<int>(0, rdds.size(), 1),
      [&](tbb::blocked_range<int> &range) {
        for (int i = range.begin(); i < range.end(); i++) {
          // TODO dirty hack :)
          auto rdd = static_cast<KeyValuesRDD<std::string, int> *>(rdds[i].get());
          rdd->GetBlocks(*block_mgr_);
          new_rdds.push_back(rdd->Reduce<std::string, int>(dl_filename));
        }
      }
  );

  return rdd_rpc::Response::OK;
}

rdd_rpc::Response Executor::GroupBy(msgpack::rpc::request &req) {
  std::cout << "groupby called" << std::endl;

  int rdd_id;
  ParseParams(req, rdd_id);

  auto &rdds = rdds_[rdd_id];
  tbb::parallel_for(
      tbb::blocked_range<int>(0, rdds.size(), 1),
      [&](tbb::blocked_range<int> &range) {
        for (int i = range.begin(); i < range.end(); i++) {
          // TODO dirty hack :)
          auto rdd = static_cast<KeyValuesRDD<std::string, int> *>(rdds[i].get());
          rdd->GetBlocks(*block_mgr_);
        }
      }
  );

  return rdd_rpc::Response::OK;
}

rdd_rpc::Response Executor::Print(msgpack::rpc::request &req) {
  std::cout << "print called" << std::endl;

  int rdd_id;
  ParseParams(req, rdd_id);

  auto &rdds = rdds_[rdd_id];
  for (const auto &rdd : rdds) {
    rdd->Print();
  }

  return rdd_rpc::Response::OK;
}
