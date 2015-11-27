//
// Created by Harunobu Daikoku on 2015/11/06.
//

#ifndef SLAVERDD_KEY_VALUES_RDD_H
#define SLAVERDD_KEY_VALUES_RDD_H

template<typename K, typename V>
class KeyValueRDD;

#include <unordered_map>
#include <vector>
#include <iostream>
#include <msgpack.hpp>
#include <reducer.h>
#include <dlfcn.h>
#include "rdd.h"
#include "../socket/socket_server.h"
#include "../socket/socket_client.h"

template<typename K, typename V>
class KeyValuesRDD: public RDD {
 public:

  KeyValuesRDD(const std::unordered_map<K, std::vector<V>> &key_values) : key_values_(key_values) { }

  KeyValuesRDD(const tbb::concurrent_unordered_map<K, tbb::concurrent_vector<V>> &key_values) {
    for (const auto &kv : key_values) {
      std::copy(kv.second.begin(), kv.second.end(), std::back_inserter(key_values_[kv.first]));
    }
  }

  template<typename NK, typename NV>
  std::unique_ptr<KeyValueRDD<NK, NV>> Reduce(const std::string &dl_filename) {
    void *handle = LoadLib(dl_filename);
    if (handle == NULL) {
      std::cerr << "dlopen" << std::endl;
      return nullptr;
    }

    const auto create_reducer
        = reinterpret_cast<CreateReducer<NK, NV, K, V>>(LoadFunc(handle, "Create"));
    if (create_reducer == nullptr) {
      std::cerr << "dlsym" << std::endl;
      dlclose(handle);
      return nullptr;
    }

    auto reducer = create_reducer();

    std::unordered_map<NK, NV> kvs;

    for (auto kv : key_values_) {
      auto reduced = reducer->Reduce(kv.first, kv.second);
      kvs.insert(reduced);
    }

    reducer.release();
    dlclose(handle);

    return std::unique_ptr<KeyValueRDD<NK, NV>>(new KeyValueRDD<NK, NV>(kvs));
  }

  bool ShuffleServer(int dest_id, int n_reducers, int port) {
    int sock_fd;
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

  bool ShuffleClient(const std::string &dst, int dest_id, int n_reducers) {
    int sock_fd;
    // TODO temporary hack for specifying port
    int port = 60090;
    SocketClient client(dst, std::to_string(port));

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

  virtual void Print() override {
    for (const auto kvs : key_values_) {
      std::cout << kvs.first << ": ";
      for (const auto v : kvs.second) {
        std::cout << v << " " << std::endl;
      }
    }
  }

 private:
  std::unordered_map<K, std::vector<V>> key_values_;

  void PackKeyValuesFor(int dest, int n_reducers, msgpack::sbuffer &sbuf) {
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

  void UnpackKeyValues(const char *buf, size_t len) {
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

};

#endif //SLAVERDD_KEY_VALUES_RDD_H