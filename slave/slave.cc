//
// Created by Harunobu Daikoku on 2015/10/26.
//
#include <iostream>
#include "slave/executor.h"

using namespace std;
using namespace msgpack;

int main(int argc, const char **argv) {
  int job_port;

  if (argc != 2) {
    cerr << "Usage: " << argv[0] << " [job_port]" << endl;
    return 1;
  }
  job_port = stoi(argv[1]);

  rpc::server job_server;
  std::unique_ptr<rpc::dispatcher> dp(new Executor("localhost", job_port));
  job_server.serve(dp.get());
  job_server.listen("0.0.0.0", job_port);
  cout << "Now Listening on port: " << job_port << endl;
  job_server.run(1);

  return 0;
}

