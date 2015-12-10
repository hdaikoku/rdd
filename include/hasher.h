//
// Created by Harunobu Daikoku on 2015/12/09.
//

#ifndef PROJECT_HASHER_H
#define PROJECT_HASHER_H

#include <cstddef>

template <typename T>
class Hasher {
 public:
  virtual std::size_t operator()(const T &t) const = 0;
};

template <>
class Hasher<std::string> {
 public:
  virtual std::size_t operator()(const std::string &t) const {
    static std::hash<std::string> hasher;
    return hasher(t);
  };
};

template <>
class Hasher<long long int> {
 public:
  virtual std::size_t operator()(const long long int &t) const {
    static std::hash<long long int> hasher;
    return hasher(t);
  };
};

template <>
class Hasher<std::pair<std::string, std::string>> {
 public:
  virtual std::size_t operator()(const std::pair<std::string, std::string> &t) const {
    std::size_t hash = 0;
    HashCombine(hash, t.first);
    HashCombine(hash, t.second);
    return hash;
  };

 protected:
  template<typename E>
  inline void HashCombine(std::size_t &seed, const E &v) const {
    std::hash<E> hasher;
    seed ^= hasher(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
  }
};

#endif //PROJECT_HASHER_H
