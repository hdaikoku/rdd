//
// Created by Harunobu Daikoku on 2015/10/26.
//

#include <chrono>
#include <iostream>
#include <rdd_context.h>

using namespace std;

int main(int argc, const char **argv) {
  if (argc != 6) {
    cerr << "Usage: " << argv[0] << " [conf_path] [text_file] [mapper.so] [combiner.so] [reducer.so]" << endl;
    return 1;
  }

  auto rc = RDDContext::NewContext(argv[1]);

  auto textFile = rc->TextFile(argv[2]);

  // begin: MapReduce
  auto start_mr = chrono::steady_clock::now();

  auto start_map = chrono::steady_clock::now();
  auto mapped = textFile->Map(argv[3], argv[4], false);
  auto end_map = chrono::steady_clock::now();

  auto start_reduce = chrono::steady_clock::now();
  auto reduced = mapped->Reduce(argv[5]);
  auto end_reduce = chrono::steady_clock::now();

  // end: MapReduce
  auto end_mr = chrono::steady_clock::now();

//  reduced->Print();

  cout << endl;

  cout << "Map: "
      << chrono::duration_cast<chrono::milliseconds>(end_map - start_map).count() / 1000.
      << " s" << endl;

  cout << "Reduce: "
      << chrono::duration_cast<chrono::milliseconds>(end_reduce - start_reduce).count() / 1000.
      << " s" << endl;

  cout << endl;

  cout << "Total MapReduce: "
      << chrono::duration_cast<chrono::milliseconds>(end_mr - start_mr).count() / 1000.
      << " s" << endl;

  return 0;
}