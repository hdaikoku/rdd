//
// Created by Harunobu Daikoku on 2016/07/26.
//

#ifndef SOCKET_SOCKET_CLIENT_H
#define SOCKET_SOCKET_CLIENT_H

#include <chrono>
#include <string>
#include <thread>

#include "socket_common.h"

#ifdef USE_RDMA
// rdma-specific functions
#define S_CONNECT(s, a, l) rconnect(s, a, l)
#define S_DST_ADDR(a)      a->ai_dst_addr
#define S_DST_ADDRLEN(a)   a->ai_dst_len
#else
// BSD socket-specific functions
#define S_CONNECT(s, a, l) connect(s, a, l)
#define S_DST_ADDR(a)      a->ai_addr
#define S_DST_ADDRLEN(a)   a->ai_addrlen
#endif //USE_RDMA

class SocketClient: public SocketCommon {
 public:
  SocketClient(const std::string &server_addr,
               uint16_t server_port,
               const std::string &log_tag = "SocketClient") : SocketCommon(log_tag) {
    SetPeerName(server_addr, server_port);
  }

  int Connect(int timeout_minutes = 1) {
    struct S_ADDRINFO *result;

    auto start = std::chrono::steady_clock::now();
    auto timeout = std::chrono::minutes(timeout_minutes);
    int backoff_msecs = 1;
    while (true) {
      if (start + timeout < std::chrono::steady_clock::now()) {
        // timed-out
        LogError("connect timed out (server:" + GetPeerNameAsString() + ")");
        return -1;
      }

      result = InitSocket(GetPeerAddr().c_str(), std::to_string(GetPeerPort()).c_str(), 0);
      if (!result) {
        return -1;
      }

      if (S_CONNECT(sock_fd_, S_DST_ADDR(result), S_DST_ADDRLEN(result)) == -1) {
        S_CLOSE(sock_fd_);
        S_FREEADDRINFO(result);
        if (errno == ECONNREFUSED) {
          // the server is not ready yet.
          std::this_thread::sleep_for(std::chrono::milliseconds(backoff_msecs));
          backoff_msecs = std::min(1000, backoff_msecs * 2);
          continue;
        } else {
          // unrecoverable error.
          LogError(errno);
          return -1;
        }
      }

      // successfully connected.
      break;
    }

    S_FREEADDRINFO(result);
    LogDebug("successfully connected to " + GetPeerNameAsString());

    return sock_fd_;
  }

  int NonBlockingConnect() {
    struct S_ADDRINFO *result;

    result = InitSocket(GetPeerAddr().c_str(), std::to_string(GetPeerPort()).c_str(), 0);
    if (!result) {
      return -1;
    }
    SetNonBlocking(true);

    int ret = -1;
    if (S_CONNECT(sock_fd_, S_DST_ADDR(result), S_DST_ADDRLEN(result)) == -1) {
      if (errno == EINPROGRESS) {
        // expected behaviour for non-blocking connect.
        LogDebug("connection to " + GetPeerNameAsString() + " will be established later");
        ret = sock_fd_;
      } else {
        // unrecoverable error.
        LogError(errno);
        S_CLOSE(sock_fd_);
      }
    } else {
      // this may happen when connection got established immediately.
      LogDebug("successfully connected to " + GetPeerNameAsString());
      ret = sock_fd_;
    }

    S_FREEADDRINFO(result);

    return ret;
  }

  const std::string &GetServerAddr() const {
    return GetPeerAddr();
  }

  uint16_t GetServerPort() const {
    return GetPeerPort();
  }

 private:
  void SetPeerName(const std::string &addr, uint16_t port) {
    peer_addr_.assign(addr);
    peer_port_ = port;
  }

};

#endif //SOCKET_SOCKET_CLIENT_H
