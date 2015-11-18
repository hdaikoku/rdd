//
// Created by Harunobu Daikoku on 2015/10/26.
//

#include <chrono>
#include <iostream>
#include <jubatus/msgpack/rpc/session_pool.h>
#include "rdd_context.h"

using namespace std;

int main(int argc, const char **argv) {
  vector<pair<string, int>> slaves;
  slaves.push_back(make_pair("127.0.0.1", 50090));
  slaves.push_back(make_pair("127.0.0.1", 50091));
  slaves.push_back(make_pair("127.0.0.1", 50092));
  slaves.push_back(make_pair("127.0.0.1", 50093));

  RddContext rc("localhost", slaves);
  rc.Init();

  auto start_text_file = chrono::steady_clock::now();
  auto textFile = rc.TextFile("/home/hdaikoku/Desktop/shared/512.txt");
  auto end_text_file = chrono::steady_clock::now();

  auto start_map = chrono::steady_clock::now();
  auto mapped = textFile->Map("/home/hdaikoku/ClionProjects/rdd/examples/libWordCount.so");
  auto end_map = chrono::steady_clock::now();

  auto start_reduce = chrono::steady_clock::now();
  auto reduced = mapped->Reduce("/home/hdaikoku/ClionProjects/rdd/examples/libWordCount.so");
  auto end_reduce = chrono::steady_clock::now();

  reduced->Print();

  cout << "TextFile: "
      << chrono::duration_cast<chrono::seconds>(end_text_file - start_text_file).count()
      << " s" << endl;

  cout << "Map: "
      << chrono::duration_cast<chrono::seconds>(end_map - start_map).count()
      << " s" << endl;

  cout << "Reduce: "
      << chrono::duration_cast<chrono::seconds>(end_reduce - start_reduce).count()
      << " s" << endl;

  return 0;
}