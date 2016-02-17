//
// Created by Harunobu Daikoku on 2015/10/29.
//

#ifndef MASTERRDD_RDD_CONTEXT_H
#define MASTERRDD_RDD_CONTEXT_H

class RDDStub;

#include <fstream>
#include <string>
#include <jubatus/msgpack/rpc/session_pool.h>
#include "master/text_file_rdd_stub.h"
#include "slave_context.h"

class RDDContext {
 public:

  static std::unique_ptr<RDDContext> NewContext(const std::string &conf_path) {
    std::ifstream ifs(conf_path);
    if (ifs.fail()) {
      return nullptr;
    }

    std::vector<SlaveContext> slaves;
    std::string addr;
    int job_port, data_port;
    while (ifs >> addr >> job_port >> data_port) {
      slaves.emplace_back(addr, job_port, data_port);
    }

    return std::unique_ptr<RDDContext>(new RDDContext(slaves));
  }

  virtual ~RDDContext() {
    sp_.end();
  }

  std::unique_ptr<TextFileRDDStub> TextFile(const std::string &filename);

  int GetNumSlaves() const;

  int GetNewRddId();

  std::string GetSlaveAddrById(const int &id) const;
  int GetSlavePortById(const int &id) const;

  void SetTimeout(int dest, unsigned int timeout);

  // Calls the endpoint specified by dest, with one argument
  template<typename A1>
  msgpack::rpc::future Call(const std::string &func, const int &dest, const A1 &a1) {
    return sp_.get_session(slaves_[dest].GetAddr(), slaves_[dest].GetJobPort()).call(func, a1);
  }

  // Calls with two args
  template<typename A1, typename A2>
  msgpack::rpc::future Call(const std::string &func, const int &dest, const A1 &a1, const A2 &a2) {
    return sp_.get_session(slaves_[dest].GetAddr(), slaves_[dest].GetJobPort()).call(func, a1, a2);
  }

  // Calls with three args
  template<typename A1, typename A2, typename A3>
  msgpack::rpc::future Call(const std::string &func, const int &dest, const A1 &a1, const A2 &a2, const A3 &a3) {
    return sp_.get_session(slaves_[dest].GetAddr(), slaves_[dest].GetJobPort()).call(func, a1, a2, a3);
  }

  // Calls with four args
  template<typename A1, typename A2, typename A3, typename A4>
  msgpack::rpc::future Call(const std::string &func,
                            const int &dest,
                            const A1 &a1,
                            const A2 &a2,
                            const A3 &a3,
                            const A4 &a4) {
    return sp_.get_session(slaves_[dest].GetAddr(), slaves_[dest].GetJobPort()).call(func, a1, a2, a3, a4);
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
    return sp_.get_session(slaves_[dest].GetAddr(), slaves_[dest].GetJobPort()).call(func, a1, a2, a3, a4, a5);
  }

 private:
  std::vector<SlaveContext> slaves_;
  msgpack::rpc::session_pool sp_;
  int last_rdd_id_;

  RDDContext(const std::vector<SlaveContext> &slaves) : slaves_(slaves) {
    Init();
  }

  void Init();

};


#endif //MASTERRDD_RDD_CONTEXT_H
