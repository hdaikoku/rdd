//
// Created by Harunobu Daikoku on 2016/11/08.
//

#ifndef SHUFFLE_IO_WORKER_CONTEXT_H
#define SHUFFLE_IO_WORKER_CONTEXT_H

#include <msgpack.hpp>
#include <string>

class WorkerContext {

 public:
  WorkerContext() {}
  WorkerContext(int worker_id, const std::string &addr, uint16_t rpc_port, uint16_t shuffle_port)
      : worker_id_(worker_id), addr_(addr), rpc_port_(rpc_port), shuffle_port_(shuffle_port) {}

  int GetWorkerID() const {
    return worker_id_;
  }

  std::string GetAddr() const {
    return addr_;
  }

  uint16_t GetRPCPort() const {
    return rpc_port_;
  }

  uint16_t GetShufflePort() const {
    return shuffle_port_;
  }

  MSGPACK_DEFINE(worker_id_, addr_, rpc_port_, shuffle_port_);

 private:
  int worker_id_;
  std::string addr_;
  uint16_t rpc_port_;
  uint16_t shuffle_port_;
};

#endif //SHUFFLE_IO_WORKER_CONTEXT_H
