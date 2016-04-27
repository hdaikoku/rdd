//
// Created by Harunobu Daikoku on 2015/10/26.
//

#include <chrono>
#include <iostream>
#include "master/rdd_context.h"

using namespace std;

int main(int argc, const char **argv) {
  if (argc != 4) {
    cerr << "Usage: " << argv[0] << " [conf_path] [text_file] [mapper.so]" << endl;
    return 1;
  }

  auto rc = RDDContext::NewContext(argv[1]);

  auto textFile = rc->TextFile(argv[2]);

  // begin: MapReduce
  auto start_mr = chrono::steady_clock::now();

  auto start_map = chrono::steady_clock::now();
  auto mapped = textFile->Map(argv[3]);
  auto end_map = chrono::steady_clock::now();

  auto start_gb = chrono::steady_clock::now();
  mapped->GroupBy();
  auto end_gb = chrono::steady_clock::now();

  // end: MapReduce
  auto end_mr = chrono::steady_clock::now();

  mapped->Print();

  cout << endl;

  cout << "Map: "
      << chrono::duration_cast<chrono::milliseconds>(end_map - start_map).count() / 1000.
      << " s" << endl;

  cout << "GroupBy: "
      << chrono::duration_cast<chrono::milliseconds>(end_gb - start_gb).count() / 1000.
      << " s" << endl;

  cout << endl;

  cout << "Total MapReduce: "
      << chrono::duration_cast<chrono::milliseconds>(end_mr - start_mr).count() / 1000.
      << " s" << endl;

  return 0;
}