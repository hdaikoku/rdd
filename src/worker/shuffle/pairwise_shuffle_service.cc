//
// Created by Harunobu Daikoku on 2016/12/16.
//

#include <future>
#include "pairwise_shuffle_service.h"

bool PairwiseShuffleService::Init(SocketSessionAcceptor &acceptor) {
  int num_clients = 0;
  auto num_workers = partition_ids_.size();

  for (int step = 1; step < num_workers; step++) {
    auto buddy_id = step ^my_executor_id_;
    auto &worker = worker_contexts_[buddy_id];
    if (my_executor_id_ < buddy_id) {
      // act as a client
      ssp_.Register(worker.GetWorkerID(), worker.GetAddr(), worker.GetShufflePort());
    } else {
      // act as a server
      num_clients++;
    }
  }

  auto f_server = std::async(std::launch::async, [&]() {
    LogInfo("launching...");
    return (acceptor.AcceptAll(num_clients) == num_clients);
  });

  if (ssp_.ConnectAll(my_executor_id_) < 0 || !f_server.get()) {
    LogError("failed to establish connections");
    return false;
  }

  auto sessions = acceptor.GetSessions();
  for (auto &&session : sessions) {
    ssp_.AddSession(session.first, std::move(session.second));
  }

  return true;
}

void PairwiseShuffleService::Start(const std::unordered_map<int, std::vector<int>> &partition_ids) {
  auto num_workers = worker_contexts_.size();
  auto &block_mgr = RDDEnv::GetInstance().GetBlockManager();

  for (int step = 1; step < num_workers; step++) {
    auto buddy_id = step ^my_executor_id_;
    auto &worker = worker_contexts_[buddy_id];
    auto &session = ssp_.GetSession(worker.GetWorkerID());
    uint32_t rlen;

    // exchange data
    if (my_executor_id_ < buddy_id) {
      // act as a client
      // send
      msgpack::sbuffer sbuf;
      std::vector<std::unique_ptr<char[]>> refs;
      auto len = block_mgr.GroupPackBlocks(partition_ids.at(buddy_id), sbuf, refs);
      session.Write(&len, sizeof(len));
      session.Write(sbuf.data(), len);
      // receive
      session.Read(&rlen, sizeof(rlen));
      std::unique_ptr<char> rbuf(new char[rlen]);
      session.Read(rbuf.get(), rlen);
      block_mgr.GroupUnpackBlocks(rbuf.get(), rlen);
    } else {
      // act as a server
      LogDebug("step " + std::to_string(step) + ": act as a server [client: " + session.GetPeerNameAsString() + "]");
      // receive
      assert(session.Read(&rlen, sizeof(rlen)) == sizeof(rlen));
      std::unique_ptr<char> rbuf(new char[rlen]);
      assert(session.Read(rbuf.get(), rlen) == rlen);
      block_mgr.GroupUnpackBlocks(rbuf.get(), rlen);
      // send
      msgpack::sbuffer sbuf;
      std::vector<std::unique_ptr<char[]>> refs;
      auto len = block_mgr.GroupPackBlocks(partition_ids.at(buddy_id), sbuf, refs);
      assert(session.Write(&len, sizeof(len)) == sizeof(len));
      assert(session.Write(sbuf.data(), len) == len);
    }
  }
}