//
// Created by Harunobu Daikoku on 2015/10/26.
//

#include <iostream>
#include <jubatus/msgpack/rpc/session_pool.h>
#include "rdd_context.h"

using namespace std;
using namespace msgpack;

int main(int argc, const char **argv) {
  vector<pair<string, int>> slaves;
  slaves.push_back(make_pair("127.0.0.1", 50090));
  slaves.push_back(make_pair("127.0.0.1", 50091));
  slaves.push_back(make_pair("127.0.0.1", 50092));
  slaves.push_back(make_pair("127.0.0.1", 50093));

  RddContext rc("localhost", slaves);
  rc.Init();
  rc.TextFile("/home/hdaikoku/Desktop/shared/512.txt")
      ->Map("/home/hdaikoku/ClionProjects/rdd/examples/libWordCount.so")
      ->Reduce("/home/hdaikoku/ClionProjects/rdd/examples/libWordCount.so")
      ->Print();

  return 0;
}