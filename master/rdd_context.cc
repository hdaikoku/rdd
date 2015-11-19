//
// Created by Harunobu Daikoku on 2015/10/29.
//

#include <fstream>
#include <jubatus/msgpack/rpc/client.h>
#include "rdd_context.h"
#include "../conn/rdd_rpc.h"

void RddContext::Init() {
  sp_.set_pool_size_limit(n_slaves_);

  std::vector<msgpack::rpc::future> fs;
  int slave_id;
  for (slave_id = 0; slave_id < n_slaves_; slave_id++) {
    fs.push_back(Call("hello", slave_id, slave_id));
  }

  for (slave_id = 0; slave_id < n_slaves_; slave_id++) {
    if (fs[slave_id].get<rdd_rpc::Response>() != rdd_rpc::Response::OK) {
      std::cerr << "could not connect to "
          << slaves_[slave_id].first << ":" << slaves_[slave_id].second << std::endl;
    }
  }
}

// Returns new RDD id
int RddContext::GetNewRddId() {
  return last_rdd_id_++;
}

std::unique_ptr<TextRddStub> RddContext::TextFile(const std::string &filename) {
  std::ifstream ifs(filename);

  std::vector<msgpack::rpc::future> fs;
  std::unique_ptr<char[]> buf(new char[default_chunk_size_ + 1]);
  std::set<int> owners;
  int owner;
  int rdd_id = GetNewRddId();

  while (!ifs.eof()) {
    if (ifs.tellg() > 0) {
      ifs.seekg(-1, ifs.cur);
      ifs.ignore(default_chunk_size_, '\n');
    }

    ifs.read(buf.get(), default_chunk_size_);
    buf[ifs.gcount()] = '\0';

    std::string last_line;
    if (!ifs.eof()) {
      std::getline(ifs, last_line);
      last_line.append("\n");
    }

    owner = next_dst_id_++ % n_slaves_;

    fs.push_back(Call("distribute", owner, rdd_id, std::string(buf.get()).append(last_line)));

    owners.insert(owner);
  }

  int i = 0;
  for (auto f : fs) {
    if (f.get<rdd_rpc::Response>() != rdd_rpc::Response::OK) {
      std::cerr << "could not distribute to "
          << slaves_[i].first << ":" << slaves_[i].second << std::endl;
    }
    i++;
  }

  return std::unique_ptr<TextRddStub>(new TextRddStub(this, rdd_id, owners));
}

std::string RddContext::GetSlaveAddrById(const int &id) {
  return slaves_[id].first;
}
