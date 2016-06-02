//
// Created by Harunobu Daikoku on 2016/02/17.
//

#include <fstream>

#include <rdd_context.h>
#include <jubatus/msgpack/rpc/client.h>

#include "rdd_rpc.h"
#include "text_file_index.h"

std::unique_ptr<TextFileRDDStub> TextFileRDDStub::NewInstance(RDDContext &rc, const std::string &filename) {
  std::vector<msgpack::rpc::future> fs;
  std::unordered_map<int, std::vector<TextFileIndex>> indices;
  auto next_dst = 0;
  auto partition_id = 0;
  auto num_slaves = rc.GetNumExecutors();
  // default chunk size: 128 MiB
  auto default_chunk_size = (1 << 27);

  std::ifstream ifs(filename);
  if (!ifs) {
    std::cerr << "Could not open file: " << filename << std::endl;
    return nullptr;
  }
  int64_t filesize = ifs.seekg(0, ifs.end).tellg();
  ifs.seekg(0, ifs.beg);

  auto rdd_id = rc.GetNewRddId();
  std::unique_ptr<TextFileRDDStub> rdd(new TextFileRDDStub(rc, rdd_id));
  while (!ifs.eof()) {
    int owner = next_dst++ % num_slaves;
    int64_t offset = ifs.tellg();

    if ((filesize - offset) < default_chunk_size) {
      if (filesize > offset) {
        indices[owner].emplace_back(partition_id, offset, (filesize - offset));
        rdd->AddPartition(owner, partition_id++);
      }
      break;
    }

    ifs.seekg(default_chunk_size, ifs.cur);

    if (!ifs.eof()) {
      ifs.ignore(default_chunk_size, '\n');
    }
    int64_t end = ifs.tellg();

    indices[owner].emplace_back(partition_id, offset, (end - offset));
    rdd->AddPartition(owner, partition_id++);
  }

  ifs.close();

  for (const auto &i : indices) {
    rc.SetTimeout(i.first, 600);
    fs.push_back(rc.Call("textfile", i.first, rdd_id, partition_id, filename, i.second));
  }

  for (auto &f : fs) {
    if (f.get<rdd_rpc::Response>() != rdd_rpc::Response::OK) {
      std::cerr << "Could not send to: " << std::endl;
      // TODO: re-distribute failed index
      continue;
    }
  }
  return std::move(rdd);
}
