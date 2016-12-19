//
// Created by Harunobu Daikoku on 2016/12/16.
//

#ifndef PROJECT_SOCKET_CONNECTION_POOL_H
#define PROJECT_SOCKET_CONNECTION_POOL_H

#include <poll.h>
#include <vector>
#include <unordered_map>

#include "logger.h"
#include "socket_client.h"

#define ERR_POLL_TIMED_OUT -1
#define ERR_POLL_FAILED    -2
#define ERR_CONN_FAILED    -3

class SocketSessionPool : public Logger {
 public:
  SocketSessionPool(const std::string &log_tag = "SocketSessionPool") : Logger(log_tag) {}

  void AddSession(int session_id, std::unique_ptr<SocketCommon> session) {
    if (sessions_.find(session_id) != sessions_.end()) {
      LogError("session " + std::to_string(session_id) + " already exists");
      return;
    }
    sessions_.emplace(std::make_pair(session_id, std::move(session)));
  }

  SocketCommon &GetSession(int session_id) {
    if (sessions_.find(session_id) == sessions_.end()) {
      LogError("Fuck");
    }
    return *sessions_[session_id];
  }

  bool Register(int session_id, const std::string &addr, uint16_t port) {
    std::unique_ptr<SocketClient> client(new SocketClient(addr, port));
    int sock_fd = client->NonBlockingConnect();
    if (sock_fd < 0) {
      LogError("failed to connect to " + addr + ":" + std::to_string(port));
      return false;
    }
    fds_.emplace_back(pollfd{sock_fd, POLLOUT, 0});
    sessions_.emplace(std::make_pair(session_id, std::move(client)));
    fd_to_session_id_.emplace(std::make_pair(sock_fd, session_id));

    return true;
  }

  int ConnectAll(int my_session_id, int timeout = 3 * 60 * 1000) {
    uint64_t num_fds;

    while ((num_fds = fds_.size()) > 0) {
      // default timeout: 3 mins
      LogDebug("polling " + std::to_string(num_fds) + " sockets...");
      auto rc = S_POLL(fds_.data(), num_fds, timeout);
      if (rc < 0) {
        LogError(errno);
        return ERR_POLL_FAILED;
      } else if (rc == 0) {
        LogError("poll timed out");
        return ERR_POLL_TIMED_OUT;
      }

      for (int i = 0; i < num_fds; i++) {
        auto &pfd = fds_[i];
        auto &session = sessions_[fd_to_session_id_[pfd.fd]];
        auto &revents = pfd.revents;

        if (revents == 0) {
          // this file descriptor is not ready yet
          continue;
        } else if (revents & (POLLHUP | POLLERR)) {
          // the socket is hanged-up or on error
          int val = 0;
          session->GetSockOpt(SOL_SOCKET, SO_ERROR, val);
          switch (val) {
            case 0:
              // no errors observed
              break;
            case ECONNREFUSED: {
              // connection has not been established yet, close connection and try again.
              LogDebug("server " + session->GetPeerNameAsString() + " is not ready yet.");
              int session_id = fd_to_session_id_[pfd.fd];
              std::string addr(session->GetPeerAddr());
              uint16_t port = session->GetPeerPort();
              Close(pfd);
              Register(session_id, addr, port);
              continue;
            }
            default:
              // unrecoverable error.
              LogError(val);
              LogError("connection to " + session->GetPeerNameAsString() + " is being closed on error");
              Close(pfd);
              return ERR_CONN_FAILED;
          }
        }

        if (revents & POLLOUT) {
          // check if connection has been established
          int val = 0;
          session->GetSockOpt(SOL_SOCKET, SO_ERROR, val);
          switch (val) {
            case 0:
              // connected.
              assert(session->WriteSome(&my_session_id, sizeof(my_session_id)) == sizeof(my_session_id));
              pfd.events &= ~POLLOUT;
              pfd.events |= POLLIN;
              break;
            case ECONNREFUSED: {
              // connection has not been established yet, close connection and try again.
              LogDebug("server " + session->GetPeerNameAsString() + " is not ready yet.");
              int session_id = fd_to_session_id_[pfd.fd];
              std::string addr(session->GetPeerAddr());
              uint16_t port = session->GetPeerPort();
              Close(pfd);
              Register(session_id, addr, port);
              continue;
            }
            default:
              // unrecoverable error.
              LogError(val);
              LogError("connection to " + session->GetPeerNameAsString() + " is being closed on error");
              Close(pfd);
              return ERR_CONN_FAILED;
          }
        }
        if (revents & POLLIN) {
          int peer_id = -1;
          assert(session->Read(&peer_id, sizeof(peer_id)) == sizeof(peer_id));
          LogInfo("connection established [" + session->GetPeerNameAsString() + "]");
          session->SetNonBlocking(false);
          pfd.fd = -1;
        }

        revents = 0;
      }

      fds_.erase(std::remove_if(fds_.begin(), fds_.end(),
                                [](const struct pollfd &fd) {
                                  return fd.fd == -1;
                                }),
                 fds_.end()
      );
      std::this_thread::yield();
    }

    return true;
  }

 private:
  std::unordered_map<int, std::unique_ptr<SocketCommon>> sessions_;
  std::vector<struct pollfd> fds_;
  std::unordered_map<int, int> fd_to_session_id_;

  void Close(struct pollfd &pfd) {
    auto session_id = fd_to_session_id_[pfd.fd];
    sessions_.erase(session_id);
    fd_to_session_id_.erase(pfd.fd);
    pfd.fd = -1;
  }

};

#endif //PROJECT_SOCKET_CONNECTION_POOL_H
