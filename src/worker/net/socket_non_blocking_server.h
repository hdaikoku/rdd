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

#define ERR_POLL_TIMED_OUT -1
#define ERR_POLL_FAILED    -2
#define ERR_ACCEPT_FAILED  -3

class SocketNonBlockingServer: public SocketServer {
 public:

  SocketNonBlockingServer(const std::string &log_tag = "SocketNonBlockingServer")
      : SocketServer(log_tag), queue_size_(0), cleanup_fds_(false) {}

  bool Init(uint16_t server_port) {
    int listen_fd = Listen(server_port);
    if (listen_fd < 0) {
      return false;
    }

    SetNonBlocking(true);
    fds_.emplace_back(pollfd{listen_fd, POLLIN, 0});
    return true;
  }

  // default poll() timeout: 3 mins
  int Run(int timeout = 3 * 60 * 1000) {
    int listen_fd = GetListenSocket();

    while (true) {
      auto num_fds = fds_.size();
      if (num_fds == 0) {
        break;
      }

      cleanup_fds_ = false;
      if (!IsRunning() && queue_size_ == 0) {
        break;
      }

      auto rc = S_POLL(fds_.data(), num_fds, timeout);
      if (rc < 0) {
        perror("poll");
        return ERR_POLL_FAILED;
      } else if (rc == 0) {
        return ERR_POLL_TIMED_OUT;
      }

      for (int i = 0; i < num_fds; i++) {
        if (fds_[i].revents == 0) {
          // this file descriptor is not ready yet
          continue;
        }

        auto revents = fds_[i].revents;
        if (revents & (POLLHUP | POLLERR)) {
          // connection has been closed
          LogDebug("connection from " + polling_socks_[fds_[i].fd]->GetPeerNameAsString() + " has been closed");
          UnregisterPollingSocket(fds_[i]);
          continue;
        }

        if (fds_[i].fd == listen_fd) {
          if (!(revents & POLLIN)) {
            continue;
          }
          // listening file descriptor is ready
          while (true) {
            auto socket = Accept();
            if (!socket) {
              if (errno != EWOULDBLOCK) {
                perror("accept");
                return ERR_ACCEPT_FAILED;
              }
              break;
            }
            RegisterPollingSocket(std::move(socket), POLLIN);
          }
        } else {
          // connected file descriptor is ready
          if (revents & POLLOUT) {
            auto &queue = send_queues_[fds_[i].fd];
            if (!OnSend(fds_[i], *polling_socks_[fds_[i].fd], queue.front())) {
              // something went wrong, connection should be closed
              UnregisterPollingSocket(fds_[i]);
            } else {
              // have successfully sent some part of the buffer
              if (queue.front().GetSize() == 0) {
                // the whole buffer has been sent, buffer should be destroyed
                auto tmp = std::move(queue.front());
                queue.pop();
                queue_size_--;
                if (queue.empty()) {
                  fds_[i].events &= ~POLLOUT;
                }
              }
            }
          }
          if (revents & POLLIN) {
            if (!OnRecv(fds_[i], *polling_socks_[fds_[i].fd])) {
              // something went wrong, connection should be closed
              UnregisterPollingSocket(fds_[i]);
            }
          }
        }

        fds_[i].revents = 0;
      }

      if (cleanup_fds_) {
        fds_.erase(std::remove_if(fds_.begin(), fds_.end(),
                                  [](const struct pollfd &fd) {
                                   return fd.fd == -1;
                                 }),
                   fds_.end()
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
  virtual void OnClose(struct pollfd &pfd) = 0;
  virtual bool IsRunning() = 0;

  void ScheduleSend(struct pollfd &pfd, SendBuffer &&buffer) {
    pfd.events |= POLLOUT;
    send_queues_[pfd.fd].push(std::move(buffer));
    queue_size_++;
  }

 private:
  std::vector<struct pollfd> fds_;
  std::unordered_map<int, std::unique_ptr<SocketCommon>> polling_socks_;
  std::unordered_map<int, std::queue<SendBuffer>> send_queues_;
  int queue_size_;
  bool cleanup_fds_;

  void RegisterPollingSocket(std::unique_ptr<SocketCommon> socket, short events) {
    auto fd = socket->GetSockFD();
    fds_.emplace_back(pollfd{fd, events, 0});
    polling_socks_.emplace(std::make_pair(fd, std::move(socket)));
  }

  void UnregisterPollingSocket(struct pollfd &pfd) {
    OnClose(pfd);
    auto queue_len = send_queues_[pfd.fd].size();
    send_queues_.erase(pfd.fd);
    queue_size_ -= queue_len;
    polling_socks_.erase(pfd.fd);
    pfd.fd = -1;
    cleanup_fds_ = true;
  }

};

#endif //SOCKET_SOCKET_NON_BLOCKING_SERVER_H
