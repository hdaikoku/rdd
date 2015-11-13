//
// Created by Harunobu Daikoku on 2015/10/26.
//
#include <iostream>
#include "executor.h"

using namespace std;
using namespace msgpack;

int main(int argc, const char **argv) {
  int job_port, data_port;

  if (argc != 3) {
    cerr << "Usage: " << argv[0] << " [job_port] [data_port]" << endl;
    return 1;
  }
  job_port = stoi(argv[1]);
  data_port = stoi(argv[2]);

  rpc::server job_server;
  std::unique_ptr<rpc::dispatcher> dp(new Executor("localhost", job_port, data_port));
  job_server.serve(dp.get());
  job_server.listen("0.0.0.0", job_port);
  job_server.run(4);

  return 0;
}

