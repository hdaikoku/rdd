//
// Created by Harunobu Daikoku on 2015/10/29.
//

#ifndef MASTERRDD_RDD_CONTEXT_H
#define MASTERRDD_RDD_CONTEXT_H

class RDDStub;

#include <fstream>
#include <string>
#include <jubatus/msgpack/rpc/session_pool.h>

#include "text_file_rdd_stub.h"
#include "worker_context.h"

class RDDContext {
 public:

  static std::unique_ptr<RDDContext>
  NewContext(const std::string &conf_path, const std::string &overlap, const std::string &shuffle_type) {
    std::ifstream ifs(conf_path);
    if (ifs.fail()) {
      return nullptr;
    }

    std::vector<WorkerContext> workers;
    std::string addr;
    uint16_t rpc_port, shuffle_port;
    int worker_id = 0;
    while (ifs >> addr >> rpc_port >> shuffle_port) {
      workers.emplace_back(worker_id++, addr, rpc_port, shuffle_port);
    }

    return std::unique_ptr<RDDContext>(new RDDContext(workers, overlap == "overlap" ? true : false, shuffle_type));
  }

  virtual ~RDDContext() {
    sp_.end();
  }

  std::unique_ptr<TextFileRDDStub> TextFile(const std::string &filename);

  int GetNumExecutors() const;

  int GetNewRddId();

  bool OverlapShuffle() const;

  std::string GetShuffleType() const;

  void SetTimeout(int dest, unsigned int timeout);

  // Calls the endpoint specified by dest, with no arguments
  msgpack::rpc::future Call(const std::string &func, const int &dest) {
    return sp_.get_session(executors_[dest].GetAddr(), executors_[dest].GetRPCPort()).call(func);
  }

  // Calls the endpoint specified by dest, with one argument
  template<typename A1>
  msgpack::rpc::future Call(const std::string &func, const int &dest, const A1 &a1) {
    return sp_.get_session(executors_[dest].GetAddr(), executors_[dest].GetRPCPort()).call(func, a1);
  }

  // Calls with two args
  template<typename A1, typename A2>
  msgpack::rpc::future Call(const std::string &func, const int &dest, const A1 &a1, const A2 &a2) {
    return sp_.get_session(executors_[dest].GetAddr(), executors_[dest].GetRPCPort()).call(func, a1, a2);
  }

  // Calls with three args
  template<typename A1, typename A2, typename A3>
  msgpack::rpc::future Call(const std::string &func, const int &dest, const A1 &a1, const A2 &a2, const A3 &a3) {
    return sp_.get_session(executors_[dest].GetAddr(), executors_[dest].GetRPCPort()).call(func, a1, a2, a3);
  }

  // Calls with four args
  template<typename A1, typename A2, typename A3, typename A4>
  msgpack::rpc::future Call(const std::string &func,
                            const int &dest,
                            const A1 &a1,
                            const A2 &a2,
                            const A3 &a3,
                            const A4 &a4) {
    return sp_.get_session(executors_[dest].GetAddr(), executors_[dest].GetRPCPort()).call(func,
                                                                                           a1,
                                                                                           a2,
                                                                                           a3,
                                                                                           a4);
  }

  // Calls with five args
  template<typename A1, typename A2, typename A3, typename A4, typename A5>
  msgpack::rpc::future Call(const std::string &func,
                            const int &dest,
                            const A1 &a1,
                            const A2 &a2,
                            const A3 &a3,
                            const A4 &a4,
                            const A5 &a5) {
    return sp_.get_session(executors_[dest].GetAddr(), executors_[dest].GetRPCPort()).call(func,
                                                                                           a1,
                                                                                           a2,
                                                                                           a3,
                                                                                           a4,
                                                                                           a5);
  }

 private:
  std::vector<WorkerContext> executors_;
  msgpack::rpc::session_pool sp_;
  int last_rdd_id_;
  bool overlap_shuffle_;
  std::string shuffle_type_;

  RDDContext(const std::vector<WorkerContext> &slaves, bool overlap_shuffle, const std::string &shuffle_type)
      : executors_(slaves), overlap_shuffle_(overlap_shuffle), shuffle_type_(shuffle_type) {
    Init();
  }

  void Init();

};


#endif //MASTERRDD_RDD_CONTEXT_H
