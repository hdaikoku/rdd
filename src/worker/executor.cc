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

    } else if (method == "map_combine") {
      // Map specified RDD (w/ combiner)
      req.result(MapCombine(req));

    } else if (method == "shuffle_srv") {
      // shuffle, act as a server
      req.result(ShuffleSrv(req));

    } else if (method == "shuffle_cli") {
      // shuffle, act as a client
      req.result(ShuffleCli(req));

    } else if (method == "stop_shuffle") {
      // stop shuffle services
      req.result(StopShuffle(req));

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
  std::cerr << "my executor_id: " << my_executor_id_ << std::endl;

  return rdd_rpc::Response::OK;
}

rdd_rpc::Response Executor::TextFile(msgpack::rpc::request &req) {
  int rdd_id;
  int num_partitions;
  std::string filename;
  std::vector<TextFileIndex> indices;
  std::unordered_map<int, std::vector<int>> partitions_by_owner;

  ParseParams(req, rdd_id, num_partitions, partitions_by_owner, filename, indices);

  RDDEnv::GetInstance().GetBlockManager().SetNumBuffers(num_partitions);

  for (const auto &index : indices) {
    rdds_[rdd_id].push_back(
        std::unique_ptr<TextFileRDD>(new TextFileRDD(num_partitions, filename, index))
    );
  }

  rdd_contexts_.emplace(std::make_pair(rdd_id, partitions_by_owner));

  return rdd_rpc::Response::OK;
}

rdd_rpc::Response Executor::Map(msgpack::rpc::request &req) {
  int rdd_id, new_rdd_id;
  std::string dl_mapper;
  ParseParams(req, rdd_id, dl_mapper, new_rdd_id);

  // load mapper library
  auto lib_mapper = UDF::NewInstance(dl_mapper);
  if (!lib_mapper) {
    std::cerr << "failed to load mapper library: " << dlerror() << std::endl;
    return rdd_rpc::Response::ERR;
  }
  auto mapper_factory = lib_mapper->LoadFunc<CreateMapper<std::string, int, int64_t, std::string>>("Create");
  if (mapper_factory == nullptr) {
    std::cerr << "failed to load Map function" << std::endl;
    return rdd_rpc::Response::ERR;
  }
  auto mapper = mapper_factory();

  auto &rdds = rdds_[rdd_id];
  auto &new_rdds = rdds_[new_rdd_id];
  tbb::parallel_for_each(rdds.begin(), rdds.end(), [&](const std::unique_ptr<RDD> &rdd) {
    // TODO dirty hack :)
    auto mapped = static_cast<TextFileRDD *>(rdd.get())
        ->Map<std::string, int>(*mapper);
    mapped->PutBlocks(RDDEnv::GetInstance().GetBlockManager());
    new_rdds.push_back(std::move(mapped));
  });

  RDDEnv::GetInstance().GetBlockManager().Finalize();

  rdd_contexts_.emplace(std::make_pair(new_rdd_id, rdd_contexts_[rdd_id]));

  return rdd_rpc::Response::OK;
}

rdd_rpc::Response Executor::MapCombine(msgpack::rpc::request &req) {
  int rdd_id, new_rdd_id;
  std::string dl_mapper, dl_combiner;
  ParseParams(req, rdd_id, dl_mapper, dl_combiner, new_rdd_id);

  // load mapper library
  auto lib_mapper = UDF::NewInstance(dl_mapper);
  if (!lib_mapper) {
    std::cerr << "failed to load mapper library: " << dlerror() << std::endl;
    return rdd_rpc::Response::ERR;
  }
  auto mapper_instance = lib_mapper->LoadFunc<CreateMapper<std::string, int, int64_t, std::string>>("Create");
  if (mapper_instance == nullptr) {
    std::cerr << "failed to load Map function" << std::endl;
    return rdd_rpc::Response::ERR;
  }
  auto mapper = mapper_instance();

  // load combiner library
  auto lib_combiner = UDF::NewInstance(dl_combiner);
  if (!lib_combiner) {
    std::cerr << "failed to load combiner library: " << dlerror() << std::endl;
    return rdd_rpc::Response::ERR;
  }
  auto combiner_instance = lib_combiner->LoadFunc<CreateReducer<std::string, int>>("Create");
  if (combiner_instance == nullptr) {
    std::cerr << "failed to load Reduce function" << std::endl;
    return rdd_rpc::Response::ERR;
  }
  auto combiner = combiner_instance();

  auto &rdds = rdds_[rdd_id];
  auto &new_rdds = rdds_[new_rdd_id];
  tbb::parallel_for_each(rdds.begin(), rdds.end(), [&](const std::unique_ptr<RDD> &rdd) {
    // TODO dirty hack :)
    auto mapped = static_cast<TextFileRDD *>(rdd.get())
        ->Map<std::string, int>(*mapper);
    if (dl_combiner != "") {
      mapped->Combine(*combiner);
    }
    mapped->PutBlocks(RDDEnv::GetInstance().GetBlockManager());
    new_rdds.push_back(std::move(mapped));
  });

  RDDEnv::GetInstance().GetBlockManager().Finalize();

  rdd_contexts_.emplace(std::make_pair(new_rdd_id, rdd_contexts_[rdd_id]));

  return rdd_rpc::Response::OK;
}

rdd_rpc::Response Executor::ShuffleSrv(msgpack::rpc::request &req) {
  int rdd_id, client_id;
  std::string shuffle_type;
  ParseParams(req, shuffle_type, rdd_id, client_id);

  if (shuffle_type == "pairwise") {
    auto partition_ids = rdd_contexts_[rdd_id][client_id];

    PairwiseShuffleServer shuffle_server(my_executor_id_);
    shuffle_server.Start(partition_ids, executors_[my_executor_id_].GetDataPort());

  } else if (shuffle_type == "fully-connected") {
    auto partitions_by_owner = rdd_contexts_[rdd_id];
    partitions_by_owner.erase(my_executor_id_);

    std::unique_ptr<FullyConnectedServer> shuffle_server(
        new FullyConnectedServer(executors_[my_executor_id_].GetDataPort(), partitions_by_owner)
    );

    RDDEnv::GetInstance().RegisterShuffleService(std::move(shuffle_server));
  }

  return rdd_rpc::Response::OK;
}

rdd_rpc::Response Executor::ShuffleCli(msgpack::rpc::request &req) {
  int rdd_id, server_id;
  std::string shuffle_type;
  ParseParams(req, shuffle_type, rdd_id, server_id);

  if (shuffle_type == "pairwise") {
    auto partition_ids = rdd_contexts_[rdd_id][server_id];

    PairwiseShuffleClient shuffle_client(my_executor_id_);
    shuffle_client.Start(partition_ids, executors_[server_id].GetAddr(), executors_[server_id].GetDataPort());

  } else if (shuffle_type == "fully-connected") {
    auto partitions_by_owner = rdd_contexts_[rdd_id];

    std::vector<std::pair<std::string, std::string>> executors;
    partitions_by_owner.erase(my_executor_id_);
    for (const auto &p : partitions_by_owner) {
      auto &owner_id = p.first;
      executors.push_back(std::make_pair(executors_[owner_id].GetAddr(), executors_[owner_id].GetDataPort()));
    }

    std::unique_ptr<FullyConnectedClient> shuffle_client(
        new FullyConnectedClient(executors, my_executor_id_)
    );

    RDDEnv::GetInstance().RegisterShuffleService(std::move(shuffle_client));
  }

  return rdd_rpc::Response::OK;
}

rdd_rpc::Response Executor::StopShuffle(msgpack::rpc::request &req) {
  RDDEnv::GetInstance().StopShuffleServices();

  return rdd_rpc::Response::OK;
}

rdd_rpc::Response Executor::Reduce(msgpack::rpc::request &req) {
  int rdd_id, new_rdd_id;
  std::string dl_reducer;
  ParseParams(req, rdd_id, dl_reducer, new_rdd_id);

  // load reducer library
  auto lib_reducer = UDF::NewInstance(dl_reducer);
  if (!lib_reducer) {
    std::cerr << "failed to load mapper library: " << dlerror() << std::endl;
    return rdd_rpc::Response::ERR;
  }
  auto reducer_instance = lib_reducer->LoadFunc<CreateReducer<std::string, int>>("Create");
  if (reducer_instance == nullptr) {
    std::cerr << "failed to load Map function" << std::endl;
    return rdd_rpc::Response::ERR;
  }
  auto reducer = reducer_instance();

  auto &rdds = rdds_[rdd_id];
  auto &new_rdds = rdds_[new_rdd_id];
  tbb::parallel_for_each(rdds.begin(), rdds.end(), [&](const std::unique_ptr<RDD> &rdd) {
    auto kvs_rdd = static_cast<KeyValuesRDD<std::string, int> *>(rdd.get());
    kvs_rdd->GetBlocks(RDDEnv::GetInstance().GetBlockManager());
    new_rdds.push_back(kvs_rdd->Reduce(*reducer));
  });

  rdd_contexts_.emplace(std::make_pair(new_rdd_id, rdd_contexts_[rdd_id]));

  return rdd_rpc::Response::OK;
}

rdd_rpc::Response Executor::GroupBy(msgpack::rpc::request &req) {
  int rdd_id;
  ParseParams(req, rdd_id);

  auto &rdds = rdds_[rdd_id];
  tbb::parallel_for_each(rdds.begin(), rdds.end(), [&](const std::unique_ptr<RDD> &rdd) {
    rdd->GetBlocks(RDDEnv::GetInstance().GetBlockManager());
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
