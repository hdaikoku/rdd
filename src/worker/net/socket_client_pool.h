//
// Created by Harunobu Daikoku on 2016/08/01.
//

#ifndef SOCKET_SOCKET_CLIENT_POOL_H
#define SOCKET_SOCKET_CLIENT_POOL_H

#include <iostream>
#include <poll.h>
#include <vector>
#include <unordered_map>

#include "socket_client.h"

class SocketClientPool {
 public:

  SocketClientPool(const std::vector<std::pair<std::string, std::string>> &servers) : servers_(servers) {}

  bool Run(int timeout = 3 * 60 * 1000) {
    std::vector<struct pollfd> fds;
    for (const auto &server : servers_) {
      std::unique_ptr<SocketClient> client(new SocketClient(server.first, server.second));
      auto sockfd = client->Connect();
      if (sockfd < 0) {
        std::cerr << "CLIENT: could not connect to one or more servers" << std::endl;
        return false;
      }

      client->SetNonBlocking(true);
      clients_.emplace(std::make_pair(sockfd, std::move(client)));
      fds.emplace_back(pollfd{sockfd, POLLOUT | POLLHUP | POLLERR, 0});
    }

    while (true) {
      auto num_fds = fds.size();
      if (num_fds == 0) {
        break;
      }

      cleanup_fds_ = false;
      // default timeout: 3 mins
      auto rc = S_POLL(fds.data(), num_fds, timeout);
      if (rc < 0) {
        perror("poll");
        break;
      } else if (rc == 0) {
        std::cerr << "poll timed out" << std::endl;
        break;
      }

      for (int i = 0; i < num_fds; i++) {
        if (fds[i].revents == 0) {
          // this file descriptor is not ready yet
          continue;
        }

        auto revents = fds[i].revents;
        if (revents & (POLLHUP | POLLERR)) {
          // connection has been closed
          Close(fds[i]);
          continue;
        } else if (revents & POLLOUT) {
          if (OnSend(fds[i], *clients_[fds[i].fd])) {
            fds[i].events &= ~POLLOUT;
          }
        } else if (revents & POLLIN) {
          if (OnRecv(fds[i], *clients_[fds[i].fd], recv_buffers_[fds[i].fd])) {
            fds[i].events &= ~POLLIN;
          }
        } else {
          // ERROR
        }

        fds[i].revents = 0;
      }

      if (cleanup_fds_) {
        fds.erase(std::remove_if(fds.begin(), fds.end(),
                                 [](const struct pollfd &fd) {
                                   return fd.fd == -1;
                                 }),
                  fds.end()
        );
      }
      std::this_thread::sleep_for(std::chrono::microseconds(500));
    }

    return true;
  }

 protected:
  class RecvBuffer {
   public:

    RecvBuffer() : size_(0), consumed_(0), tag_(0) {}

    void Reset(int32_t size, int tag) {
      size_ = size;
      if (size > buf_.capacity()) {
        buf_.reserve(size);
      }
      consumed_ = 0;
      tag_ = tag;
    }

    char *Get() {
      return buf_.data() + consumed_;
    }

    int32_t GetSize() const {
      return (size_ - consumed_);
    }

    int32_t GetCapacity() const {
      return size_;
    }

    int GetTag() const {
      return tag_;
    }

    void Consumed(int32_t consumed) {
      consumed_ += consumed;
    }

    template<typename T>
    const T &As() const {
      return *reinterpret_cast<const T *>(buf_.data());
    }

    const char *Data() const {
      return buf_.data();
    }

   private:
    std::vector<char> buf_;
    int32_t size_;
    int32_t consumed_;
    int tag_;
  };

  void Close(struct pollfd &pfd) {
    cleanup_fds_ = true;
    clients_.erase(pfd.fd);
    pfd.fd = -1;
  }

  virtual bool OnRecv(struct pollfd &pfd, const SocketCommon &socket, RecvBuffer &rbuffer) = 0;
  virtual bool OnSend(struct pollfd &pfd, const SocketCommon &socket) = 0;

  void ScheduleSend(struct pollfd &pfd) {
    pfd.events |= POLLOUT;
  }

  void ScheduleRecv(struct pollfd &pfd, int32_t buf_size, int tag) {
    recv_buffers_[pfd.fd].Reset(buf_size, tag);
    pfd.events |= POLLIN;
  }

 private:
  std::vector<std::pair<std::string, std::string>> servers_;
  std::unordered_map<int, RecvBuffer> recv_buffers_;
  std::unordered_map<int, std::unique_ptr<SocketClient>> clients_;

  bool cleanup_fds_;
};

#endif //SOCKET_SOCKET_CLIENT_POOL_H
