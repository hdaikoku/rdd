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
  slaves.push_back(make_pair("localhost", 50090));
  slaves.push_back(make_pair("localhost", 50091));
  slaves.push_back(make_pair("localhost", 50092));

  RddContext rc("localhost", slaves);
  rc.Init();
  rc.TextFile("/Users/HDaikoku/Desktop/shared/256.txt")->Map(
      "/Users/HDaikoku/ClionProjects/RDD/examples/libWordCount.dylib");

  return 0;
}