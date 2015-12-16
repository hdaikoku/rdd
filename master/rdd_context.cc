//
// Created by Harunobu Daikoku on 2015/10/29.
//

#include <fstream>
#include <jubatus/msgpack/rpc/client.h>
#include "master/rdd_context.h"
#include "rdd_rpc.h"

void RDDContext::Init() {
  // default size of chunks: 128 MB
  default_chunk_size_ = (1 << 27);
  last_rdd_id_ = 0;
  next_dst_id_ = 0;
  n_slaves_ = slaves_.size();

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

void RDDContext::SetTimeout(int dest, unsigned int timeout) {
  sp_.get_session(slaves_[dest].first, slaves_[dest].second).set_timeout(timeout);
}

// Returns new RDD id
int RDDContext::GetNewRddId() {
  return last_rdd_id_++;
}

std::unique_ptr<KeyValueRDDStub> RDDContext::TextFile(const std::string &filename) {
  std::ifstream ifs(filename);

  std::vector<msgpack::rpc::future> fs;
  std::unordered_map<int, std::vector<std::pair<long long int, int>>> index;
  std::set<int> owners;
  int owner;
  int rdd_id = GetNewRddId();

  long long int filesize = ifs.seekg(0, ifs.end).tellg();
  ifs.seekg(0, ifs.beg);

  while (!ifs.eof()) {
    owner = next_dst_id_++ % n_slaves_;

    long long int offset = ifs.tellg();

    if ((filesize - offset) < default_chunk_size_) {
      if (filesize > offset) {
        index[owner].push_back(std::make_pair(offset, (filesize - offset)));
      }
      break;
    }

    ifs.seekg(default_chunk_size_, ifs.cur);

    if (!ifs.eof()) {
      ifs.ignore(default_chunk_size_, '\n');
    }
    long long int end = ifs.tellg();

    index[owner].push_back(std::make_pair(offset, (end - offset)));
  }

  ifs.close();

  for (const auto &i : index) {
    fs.push_back(Call("distribute", i.first, rdd_id, filename, i.second));
  }

  int i = 0;
  for (auto f : fs) {
    if (f.get<rdd_rpc::Response>() != rdd_rpc::Response::OK) {
      std::cerr << "could not distribute to "
          << slaves_[i].first << ":" << slaves_[i].second << std::endl;
    }
    owners.insert(i);
    i++;
  }

  return std::unique_ptr<KeyValueRDDStub>(new KeyValueRDDStub(this, rdd_id, owners));
}

std::string RDDContext::GetSlaveAddrById(const int &id) {
  return slaves_[id].first;
}
