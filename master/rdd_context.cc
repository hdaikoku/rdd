//
// Created by Harunobu Daikoku on 2015/10/29.
//

#include <fstream>
#include <jubatus/msgpack/rpc/client.h>
#include "rdd_context.h"
#include "rdd_rpc.h"

void RddContext::Init() {
  int i = 0;
  int n_slaves = slaves_.size();
//  signal(SIGPIPE, SIG_IGN);

  s_pool_.set_pool_size_limit(n_slaves);

  std::vector<msgpack::rpc::future> fs;
  for (auto s : slaves_) {
    msgpack::rpc::session session = s_pool_.get_session(s.first, s.second);

    std::cout << "calling " << s.first << ":" << s.second << std::endl;
    fs.push_back(session.call("hello", i));

    i++;
  }

  i = 0;
  for (auto f : fs) {
    if (f.get<rdd_rpc::Response>() != rdd_rpc::Response::OK) {
      std::cerr << "could not connect to " << slaves_[i].first << ":" << slaves_[i].second << std::endl;
    }
    i++;
  }
}

bool RddContext::CallAll(const std::string &func) {
  std::vector<msgpack::rpc::future> fs;
  bool ret = true;

  for (auto slave : slaves_) {
    fs.push_back(s_pool_.get_session(slave.first, slave.second).call(func));
  }

  for (auto f : fs) {
    if (f.get<std::string>() != "ok") {
      ret = false;
    }
  }

  return ret;
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
  int owner = 0;
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

    msgpack::rpc::session session = s_pool_.get_session(slaves_[owner].first, slaves_[owner].second);
    fs.push_back(session.call("distribute", rdd_id, std::string(buf.get()).append(last_line)));

    owners.insert(owner);
    owner++;
  }

  int i = 0;
  for (auto f : fs) {
    if (f.get<rdd_rpc::Response>() != rdd_rpc::Response::OK) {
      std::cerr << "could not distribute to " << slaves_[i].first << ":" << slaves_[i].second << std::endl;
    }
    i++;
  }

  return std::unique_ptr<TextRddStub>(new TextRddStub(this, rdd_id, owners));
}

std::string RddContext::GetSlaveAddrById(const int &id) {
  return slaves_[id].first;
}
