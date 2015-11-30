//
// Created by Harunobu Daikoku on 2015/10/29.
//

#ifndef MASTERRDD_RDD_CONTEXT_H
#define MASTERRDD_RDD_CONTEXT_H

#include <string>
#include <jubatus/msgpack/rpc/session_pool.h>
#include "key_value_rdd_stub.h"

class RDDContext {
 public:
  RDDContext(const std::string &master_addr, const std::vector<std::pair<std::string, int>> &slaves)
      : master_addr_(master_addr), slaves_(slaves) {
    // default size of chunks: 128 MB
    default_chunk_size_ = (1 << 27);
    last_rdd_id_ = 0;
    next_dst_id_ = 0;
    n_slaves_ = slaves_.size();
  }

  virtual ~RDDContext() {
    sp_.end();
  }

  void Init();

  void SetTimeout(int dest, unsigned int timeout);

  // Calls the endpoint specified by dest, with one argument
  template<typename A1>
  msgpack::rpc::future Call(const std::string &func, const int &dest, const A1 &a1) {
    return sp_.get_session(slaves_[dest].first, slaves_[dest].second).call(func, a1);
  }

  // Calls with two args
  template<typename A1, typename A2>
  msgpack::rpc::future Call(const std::string &func, const int &dest, const A1 &a1, const A2 &a2) {
    return sp_.get_session(slaves_[dest].first, slaves_[dest].second).call(func, a1, a2);
  }

  // Calls with three args
  template<typename A1, typename A2, typename A3>
  msgpack::rpc::future Call(const std::string &func, const int &dest, const A1 &a1, const A2 &a2, const A3 &a3) {
    return sp_.get_session(slaves_[dest].first, slaves_[dest].second).call(func, a1, a2, a3);
  }

  // Calls with four args
  template<typename A1, typename A2, typename A3, typename A4>
  msgpack::rpc::future Call(const std::string &func,
                            const int &dest,
                            const A1 &a1,
                            const A2 &a2,
                            const A3 &a3,
                            const A4 &a4) {
    return sp_.get_session(slaves_[dest].first, slaves_[dest].second).call(func, a1, a2, a3, a4);
  }


  int GetNewRddId();

  std::string GetSlaveAddrById(const int &id);

  std::unique_ptr<KeyValueRDDStub> TextFile(const std::string &filename);

 private:
  std::string master_addr_;
  std::vector<std::pair<std::string, int>> slaves_;
  int n_slaves_;
  int next_dst_id_;
  msgpack::rpc::session_pool sp_;

  int default_chunk_size_;
  int last_rdd_id_;
};


#endif //MASTERRDD_RDD_CONTEXT_H
