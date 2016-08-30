//
// Created by Harunobu Daikoku on 2016/08/01.
//

#ifndef SOCKET_SOCKET_NON_BLOCKING_SERVER_H
#define SOCKET_SOCKET_NON_BLOCKING_SERVER_H

#include <iostream>
#include <poll.h>
#include <queue>
#include <unordered_map>
#include <vector>

#include "socket_server.h"

class SocketNonBlockingServer: public SocketServer {
 public:

  SocketNonBlockingServer(const std::string &server_port)
      : SocketServer(server_port), queue_size_(0) {}

  // default poll() timeout: 3 mins
  bool Run(int timeout = 3 * 60 * 1000) {
    auto listen_fd = Listen();
    SetNonBlocking(true);
    fds.emplace_back(pollfd{listen_fd, POLLIN | POLLHUP | POLLERR, 0});

    while (true) {
      auto num_fds = fds.size();
      if (num_fds == 0) {
        break;
      }

      bool cleanup = false;
      if (!IsRunning() && queue_size_ == 0) {
        break;
      }

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
          UnregisterPollingSocket(fds[i]);
          cleanup = true;
          continue;
        }

        if (fds[i].fd == listen_fd) {
          if (!(revents & POLLIN)) {
            continue;
          }
          // listening file descriptor is ready
          while (true) {
            auto socket = Accept();
            if (!socket) {
              if (errno != EWOULDBLOCK) {
                perror("accept");
              }
              break;
            }
            RegisterPollingSocket(std::move(socket), POLLIN);
          }
        } else {
          // connected file descriptor is ready
          if (revents & POLLOUT) {
            auto &queue = send_queues_[fds[i].fd];
            if (OnSend(fds[i], *polling_socks_[fds[i].fd], queue.front())) {
              auto buf = std::move(queue.front());
              queue.pop();
              queue_size_--;
              if (queue.empty()) {
                fds[i].events &= ~POLLOUT;
              }
            }
          }
          if (revents & POLLIN) {
            if (!OnRecv(fds[i], *polling_socks_[fds[i].fd])) {
              UnregisterPollingSocket(fds[i]);
              cleanup = true;
            }
          }
        }

        fds[i].revents = 0;
      }

      if (cleanup) {
        fds.erase(std::remove_if(fds.begin(), fds.end(),
                                 [](const struct pollfd &fd) {
                                   return fd.fd == -1;
                                 }),
                  fds.end()
        );
      }
    }

    return true;
  }

 protected:

  class SendBuffer {
   public:

    SendBuffer() : size_(0), consumed_(0) {}

    SendBuffer(std::unique_ptr<char[]> data, int32_t size)
        : data_(std::move(data)), size_(size), consumed_(0) {}

    SendBuffer(void *data, int32_t size) : data_(new char[size]), size_(size), consumed_(0) {
      std::memcpy(data_.get(), data, size);
    }

    void *Get() const {
      return data_.get() + consumed_;
    }

    int32_t GetSize() const {
      return size_ - consumed_;
    }

    void Consumed(int32_t consumed) {
      consumed_ += consumed;
    }

   private:
    std::unique_ptr<char[]> data_;
    int32_t size_;
    int32_t consumed_;
  };

  virtual bool OnRecv(struct pollfd &pfd, const SocketCommon &socket) = 0;
  virtual bool OnSend(struct pollfd &pfd, const SocketCommon &socket, SendBuffer &send_buffer) = 0;
  virtual bool IsRunning() = 0;

  void ScheduleSend(struct pollfd &pfd, SendBuffer &&buffer) {
    pfd.events |= POLLOUT;
    send_queues_[pfd.fd].push(std::move(buffer));
    queue_size_++;
  }

 private:
  std::vector<struct pollfd> fds;
  std::unordered_map<int, std::unique_ptr<SocketCommon>> polling_socks_;
  std::unordered_map<int, std::queue<SendBuffer>> send_queues_;
  int queue_size_;

  void RegisterPollingSocket(std::unique_ptr<SocketCommon> socket, short events) {
    auto fd = socket->GetSockFD();
    fds.emplace_back(pollfd{fd, events, 0});
    polling_socks_.emplace(std::make_pair(fd, std::move(socket)));
  }

  void UnregisterPollingSocket(struct pollfd &fd) {
    polling_socks_.erase(fd.fd);
    fd.fd = -1;
  }

};

#endif //SOCKET_SOCKET_NON_BLOCKING_SERVER_H
