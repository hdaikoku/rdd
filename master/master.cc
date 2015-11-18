//
// Created by Harunobu Daikoku on 2015/10/26.
//

#include <chrono>
#include <iostream>
#include <jubatus/msgpack/rpc/session_pool.h>
#include <fstream>
#include <sstream>
#include "rdd_context.h"

using namespace std;

static bool ReadConf(const char *conf_path, vector<pair<string, int>> &slaves) {
  ifstream ifs(conf_path, ios::in);
  if (ifs.fail()) {
    return false;
  }

  string line, addr, port;
  while (getline(ifs, line)) {
    istringstream splitter(line);
    getline(splitter, addr, ' ');
    getline(splitter, port, ' ');
    slaves.push_back(make_pair(addr, stoi(port)));
  }

  return true;
}

int main(int argc, const char **argv) {
  if (argc != 4) {
    cerr << "Usage: " << argv[0] << "[conf_path] [text_file] [map_reduce.so]" << endl;
    return 1;
  }

  vector<pair<string, int>> slaves;
  if (!ReadConf(argv[1], slaves)) {
    cerr << "could not read the conf file: " << argv[1] << endl;
    return 1;
  }

  RddContext rc("localhost", slaves);
  rc.Init();

  auto start_text_file = chrono::steady_clock::now();
  auto textFile = rc.TextFile(argv[2]);
  auto end_text_file = chrono::steady_clock::now();

  auto start_map = chrono::steady_clock::now();
  auto mapped = textFile->Map(argv[3]);
  auto end_map = chrono::steady_clock::now();

  auto start_reduce = chrono::steady_clock::now();
  auto reduced = mapped->Reduce(argv[3]);
  auto end_reduce = chrono::steady_clock::now();

  reduced->Print();

  cout << "TextFile: "
      << chrono::duration_cast<chrono::milliseconds>(end_text_file - start_text_file).count()
      << " s" << endl;

  cout << "Map: "
      << chrono::duration_cast<chrono::milliseconds>(end_map - start_map).count()
      << " s" << endl;

  cout << "Reduce: "
      << chrono::duration_cast<chrono::milliseconds>(end_reduce - start_reduce).count()
      << " s" << endl;

  return 0;
}