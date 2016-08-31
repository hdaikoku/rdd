//
// Created by Harunobu Daikoku on 2015/10/26.
//

#include <chrono>
#include <iostream>
#include <rdd_context.h>

using namespace std;

int main(int argc, const char **argv) {
  if (argc != 7) {
    cerr << "Usage: " << argv[0] << " [conf_path] [text_file] [mapper.so] [reducer.so] [overlap?] [shuffle_type]"
         << endl;
    return 1;
  }

  auto rc = RDDContext::NewContext(argv[1], argv[5], argv[6]);

  auto textFile = rc->TextFile(argv[2]);

  auto start_map = chrono::steady_clock::now();
  auto mapped = textFile->Map(argv[3], argv[4]);
  auto end_map = chrono::steady_clock::now();

  auto start_reduce = chrono::steady_clock::now();
  auto reduced = mapped->Reduce(argv[4]);
  auto end_reduce = chrono::steady_clock::now();

  reduced->Print();

  cout << endl;

  cout << "Shuffle Type: "
       << (rc->OverlapShuffle() ? "Fully-Connected w/ overlapping" : rc->GetShuffleType()) << std::endl;

  cout << "Map: "
      << chrono::duration_cast<chrono::milliseconds>(end_map - start_map).count() / 1000.
      << " s" << endl;

  cout << "Reduce: "
      << chrono::duration_cast<chrono::milliseconds>(end_reduce - start_reduce).count() / 1000.
      << " s" << endl;

  cout << "Total MapReduce: "
      << chrono::duration_cast<chrono::milliseconds>(end_reduce - start_map).count() / 1000.
      << " s" << endl;

  return 0;
}