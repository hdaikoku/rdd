//
// Created by Harunobu Daikoku on 2015/10/29.
//

#ifndef SLAVERDD_EXECUTOR_H
#define SLAVERDD_EXECUTOR_H

#include <jubatus/msgpack/rpc/server.h>
#include <tbb/tbb.h>

#include "rdd_rpc.h"
#include "worker_context.h"
#include "worker/rdd.h"

class Executor: public msgpack::rpc::dispatcher {

 public:
  Executor(const std::string &addr, int job_port)
      : addr_(addr), job_port_(job_port) { }

  virtual void dispatch(msgpack::rpc::request req) override;

 private:
  std::string addr_;
  int job_port_;
  int my_executor_id_;
  std::vector<WorkerContext> executors_;
  std::unordered_map<int, tbb::concurrent_vector<std::unique_ptr<RDD>>> rdds_;
  std::unique_ptr<BlockManager> block_mgr_;

  rdd_rpc::Response Hello(msgpack::rpc::request &req);
  rdd_rpc::Response TextFile(msgpack::rpc::request &req);
  rdd_rpc::Response Map(msgpack::rpc::request &req);
  rdd_rpc::Response MapWithShuffle(msgpack::rpc::request &req);
  rdd_rpc::Response ShuffleSrv(msgpack::rpc::request &req);
  rdd_rpc::Response ShuffleCli(msgpack::rpc::request &req);
  rdd_rpc::Response Reduce(msgpack::rpc::request &req);
  rdd_rpc::Response GroupBy(msgpack::rpc::request &req);
  rdd_rpc::Response Print(msgpack::rpc::request &req);

  template<typename P1>
  void ParseParams(msgpack::rpc::request &req, P1 &p1) const {
    msgpack::type::tuple<P1> params;

    req.params().convert(&params);
    p1 = params.template get<0>();
  }

  template<typename P1, typename P2>
  void ParseParams(msgpack::rpc::request &req, P1 &p1, P2 &p2) const {
    msgpack::type::tuple<P1, P2> params;

    req.params().convert(&params);
    p1 = params.template get<0>();
    p2 = params.template get<1>();
  }

  template<typename P1, typename P2, typename P3>
  void ParseParams(msgpack::rpc::request &req, P1 &p1, P2 &p2, P3 &p3) const {
    msgpack::type::tuple<P1, P2, P3> params;

    req.params().convert(&params);
    p1 = params.template get<0>();
    p2 = params.template get<1>();
    p3 = params.template get<2>();
  }

  template<typename P1, typename P2, typename P3, typename P4>
  void ParseParams(msgpack::rpc::request &req, P1 &p1, P2 &p2, P3 &p3, P4 &p4) const {
    msgpack::type::tuple<P1, P2, P3, P4> params;

    req.params().convert(&params);
    p1 = params.template get<0>();
    p2 = params.template get<1>();
    p3 = params.template get<2>();
    p4 = params.template get<3>();
  }

  template<typename P1, typename P2, typename P3, typename P4, typename P5>
  void ParseParams(msgpack::rpc::request &req, P1 &p1, P2 &p2, P3 &p3, P4 &p4, P5 &p5) const {
    msgpack::type::tuple<P1, P2, P3, P4, P5> params;

    req.params().convert(&params);
    p1 = params.template get<0>();
    p2 = params.template get<1>();
    p3 = params.template get<2>();
    p4 = params.template get<3>();
    p5 = params.template get<4>();
  }

};


#endif //SLAVERDD_EXECUTOR_H
