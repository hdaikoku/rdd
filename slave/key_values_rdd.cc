//
// Created by Harunobu Daikoku on 2015/11/06.
//

#include "key_values_rdd.h"
#include "../socket/socket_server.h"
#include "../socket/socket_client.h"

template<typename K, typename V>
std::unique_ptr<KeyValuesRdd<K, V>> KeyValuesRdd<K, V>::Reduce(const std::string &dl_filename) {
  void *handle = LoadLib(dl_filename);
  if (handle == NULL) {
    std::cerr << "dlopen" << std::endl;
    return nullptr;
  }

  const auto create_map_reduce
      = reinterpret_cast<CreateMapReduce<K, V, std::string>>(LoadFunc(handle, "Create"));
  if (create_map_reduce == nullptr) {
    std::cerr << "dlsym" << std::endl;
    dlclose(handle);
    return nullptr;
  }

  auto map_reduce = create_map_reduce();

  std::unordered_map<K, std::vector<V>> kvs;

  for (auto kv : key_values_) {
    auto reduced = map_reduce->Reduce(kv.first, kv.second);
    kvs[reduced.first].push_back(reduced.second);
  }

  map_reduce.release();
  dlclose(handle);

  return std::unique_ptr<KeyValuesRdd<K, V>>(new KeyValuesRdd<K, V>(kvs));
}

template<typename K, typename V>
bool KeyValuesRdd<K, V>::ShuffleServer(int dest_id, int n_reducers, int port) {
  int sock_fd;
  // TODO temporary hack for specifying port
  SocketServer server(std::to_string(port));

  if (!server.Listen()) {
    std::cerr << "listen failed: " << port << std::endl;
    return false;
  }
  std::cout << "listening: " << port << std::endl;

  if ((sock_fd = server.Accept()) < 0) {
    perror("raccept");
    return false;
  }

  size_t len = 0;
  auto rbuf = server.ReadWithProbe(sock_fd, len);
  if (!rbuf) {
    std::cerr << "read failed" << std::endl;
    return false;
  }

  //  prepare for receiving the data
  UnpackKeyValues(rbuf.get(), len);
  rbuf.reset(nullptr);

  msgpack::sbuffer sbuf;
  PackKeyValuesFor(dest_id, n_reducers, sbuf);

  if (server.WriteWithProbe(sock_fd, sbuf.data(), sbuf.size()) < 0) {
    std::cerr << "write failed" << std::endl;
    return false;
  }

  return true;
}

template<typename K, typename V>
bool KeyValuesRdd<K, V>::ShuffleClient(const std::string &dst, int dest_id, int n_reducers) {
  int sock_fd;
  // TODO temporary hack for specifying port
  int port = 60090 + dest_id;
  SocketClient client(dst, std::to_string(port));

  sleep(3);

  if ((sock_fd = client.Connect()) < 0) {
    std::cerr << "could not connect to: " << dst << ":" << port << std::endl;
    return false;
  }

  msgpack::sbuffer sbuf;
  PackKeyValuesFor(dest_id, n_reducers, sbuf);

  if (client.WriteWithProbe(sock_fd, sbuf.data(), sbuf.size()) < 0) {
    std::cerr << "write failed" << std::endl;
    return false;
  }
  free(sbuf.release());

  size_t len = 0;
  auto rbuf = client.ReadWithProbe(sock_fd, len);
  if (!rbuf) {
    std::cerr << "read failed" << std::endl;
    return false;
  }

  //  prepare for receiving the data
  UnpackKeyValues(rbuf.get(), len);
  rbuf.reset(nullptr);

  return true;
}

template<typename K, typename V>
void KeyValuesRdd<K, V>::PackKeyValuesFor(int dest, int n_reducers, msgpack::sbuffer &sbuf) {
  std::hash<K> hasher;

  auto iter = key_values_.begin();
  while (iter != key_values_.end()) {
    if ((hasher(iter->first) % n_reducers) == dest) {
      msgpack::pack(&sbuf, *iter);
      iter = key_values_.erase(iter);
    } else {
      iter++;
    }
  }
}

template<typename K, typename V>
void KeyValuesRdd<K, V>::UnpackKeyValues(const char *buf, size_t len) {
  msgpack::unpacker upc;
  upc.reserve_buffer(len);
  memcpy(upc.buffer(), buf, len);
  upc.buffer_consumed(len);

  msgpack::unpacked result;
  while (upc.next(&result)) {
    std::pair<K, std::vector<V>> received;
    result.get().convert(&received);
    std::copy(received.second.begin(), received.second.end(),
              std::back_inserter(key_values_[received.first]));
  }
}
