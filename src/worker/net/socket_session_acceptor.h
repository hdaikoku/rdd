//
// Created by Harunobu Daikoku on 2016/12/17.
//

#ifndef PROJECT_SOCKET_SESSION_ACCEPTOR_H
#define PROJECT_SOCKET_SESSION_ACCEPTOR_H

#include <poll.h>
#include <queue>
#include <unordered_map>
#include <vector>

#include "socket_server.h"

#define ERR_POLL_TIMED_OUT -1
#define ERR_POLL_FAILED    -2
#define ERR_ACCEPT_FAILED  -3

class SocketSessionAcceptor : public SocketServer {
 public:

  SocketSessionAcceptor(const std::string &log_tag = "SocketSessionAcceptor")
      : SocketServer(log_tag), cleanup_fds_(false) {}

  std::vector<std::pair<int, std::unique_ptr<SocketCommon>>> GetSessions() {
    std::vector<std::pair<int, std::unique_ptr<SocketCommon>>> sessions;
    for (auto &&session : polling_socks_) {
      sessions.emplace_back(std::make_pair(session_ids_[session.first], std::move(session.second)));
    }

    return std::move(sessions);
  }

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
  int AcceptAll(int num_clients, int timeout = 3 * 60 * 1000) {
    int num_accepted = 0;

    while (num_accepted < num_clients) {
      cleanup_fds_ = false;

      auto num_fds = fds_.size();
      auto rc = S_POLL(fds_.data(), num_fds, timeout);
      if (rc < 0) {
        perror("poll");
        return ERR_POLL_FAILED;
      } else if (rc == 0) {
        return ERR_POLL_TIMED_OUT;
      }

      for (int i = 0; i < num_fds; i++) {
        auto &pfd = fds_[i];
        auto &revents = pfd.revents;

        if (revents == 0) {
          // this file descriptor is not ready yet
          continue;
        }

        if (revents & (POLLHUP | POLLERR)) {
          // connection has been closed
          LogError("connection from " + polling_socks_[pfd.fd]->GetPeerNameAsString() + " has been closed");
          return ERR_POLL_FAILED;
        }

        if (pfd.fd == sock_fd_) {
          if (revents & POLLIN) {
            // listening file descriptor is ready
            while (true) {
              auto socket = Accept();
              if (!socket) {
                if (errno != EWOULDBLOCK) {
                  LogError("accept failed");
                  return ERR_ACCEPT_FAILED;
                }
                break;
              }
              RegisterPollingSocket(std::move(socket), POLLIN);
            }
          }
        } else {
          // connected file descriptor is ready
          auto &session = polling_socks_[pfd.fd];
          if (revents & POLLIN) {
            int peer_id = -1;
            assert(session->Read(&peer_id, sizeof(peer_id)) == sizeof(peer_id));
            session_ids_.emplace(std::make_pair(pfd.fd, peer_id));
            pfd.events &= ~POLLIN;
            pfd.events |= POLLOUT;
          }
          if (revents & POLLOUT) {
            int peer_id = 0;
            assert(session->Write(&peer_id, sizeof(peer_id)) == sizeof(peer_id));
            session->SetNonBlocking(false);
            UnregisterPollingSocket(pfd);
            num_accepted++;
          }
        }

        revents = 0;
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

    return num_accepted;
  }

 private:
  bool cleanup_fds_;
  std::vector<struct pollfd> fds_;
  std::unordered_map<int, int> session_ids_;
  std::unordered_map<int, std::unique_ptr<SocketCommon>> polling_socks_;

  void RegisterPollingSocket(std::unique_ptr<SocketCommon> socket, short events) {
    auto fd = socket->GetSockFD();
    fds_.emplace_back(pollfd{fd, events, 0});
    polling_socks_.emplace(std::make_pair(fd, std::move(socket)));
  }

  void UnregisterPollingSocket(struct pollfd &pfd) {
    pfd.fd = -1;
    cleanup_fds_ = true;
  }

};

#endif //PROJECT_SOCKET_SESSION_ACCEPTOR_H
