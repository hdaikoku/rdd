//
// Created by Harunobu Daikoku on 2016/04/08.
//

#ifndef FULLY_CONNECTED_POLL_NON_BLOCKING_SERVER_H
#define FULLY_CONNECTED_POLL_NON_BLOCKING_SERVER_H

#include <algorithm>
#include <vector>
#include <thread>
#include <unordered_map>
#include <poll.h>
#include <queue>
#include "shuffle/send_buffer.h"
#include "socket/socket_server.h"

class NonBlockingServer: public SocketServer {

 public:
  NonBlockingServer(const int server_port)
      : SocketServer(server_port) { }

 protected:
  virtual bool OnRecv(struct pollfd &pfd) = 0;
  virtual bool OnSend(struct pollfd &pfd, SendBuffer &send_buffer) = 0;
  virtual bool IsRunning() = 0;

  void ScheduleSend(struct pollfd &pfd, SendBuffer &&buffer) {
    pfd.events |= POLLOUT;
    send_queues_[pfd.fd].push(std::move(buffer));
  }

  bool Run() {
    Listen();
    auto listen_fd = GetListenSocket();
    SetNonBlocking(listen_fd);

    fds.emplace_back(pollfd{listen_fd, POLLIN | POLLHUP, 0});

    while (fds.size() > 0) {
      if (!IsRunning() && EmptyQueues()) {
        break;
      }

      // default timeout: 3 mins
      auto rc = poll(fds.data(), fds.size(), 3 * 60 * 1000);
      if (rc < 0) {
        perror("poll");
        break;
      } else if (rc == 0) {
        std::cerr << "poll timed out" << std::endl;
        break;
      }

      auto current_size = fds.size();
      for (int i = 0; i < current_size; i++) {
        if (fds[i].revents == 0) {
          // this file descriptor is not ready yet
          continue;
        }

        auto revents = fds[i].revents;
        if (revents & POLLHUP) {
          // connection has been closed
          close(fds[i].fd);
          fds[i].fd = -1;
          continue;
        }

        if (fds[i].fd == listen_fd) {
          if (!(revents & POLLIN)) {
            continue;
          }
          // listening file descriptor is ready
          while (true) {
            auto new_fd = Accept(listen_fd);
            if (new_fd < 0) {
              if (errno != EWOULDBLOCK) {
                perror("accept");
              }
              break;
            }
            fds.emplace_back(pollfd{new_fd, POLLIN, 0});
          }
        } else {
          // connected file descriptor is ready
          if (revents & POLLOUT) {
            if (OnSend(fds[i], send_queues_[fds[i].fd].front())) {
              auto buf = std::move(send_queues_[fds[i].fd].front());
              send_queues_[fds[i].fd].pop();
              if (send_queues_[fds[i].fd].empty()) {
                fds[i].events &= ~POLLOUT;
              }
            }
          }
          if (revents & POLLIN) {
            if (!OnRecv(fds[i])) {
              close(fds[i].fd);
              fds[i].fd = -1;
            }
          }
        }

        fds[i].revents = 0;
      }

      fds.erase(std::remove_if(fds.begin(),
                               fds.end(),
                               [](const struct pollfd &fd) {
                                 return fd.fd == -1;
                               }),
                fds.end()
      );
    }

    return true;
  }

  void Break() {
    for (auto &fd : fds) {
      close(fd.fd);
      fd.fd = -1;
    }
  }

 private:
  std::vector<struct pollfd> fds;
  std::unordered_map<int, std::queue<SendBuffer>> send_queues_;

  bool EmptyQueues() const {
    for (const auto &queue : send_queues_) {
      if (!queue.second.empty()) {
        return false;
      }
    }

    return true;
  }

};

#endif //FULLY_CONNECTED_POLL_NON_BLOCKING_SERVER_H