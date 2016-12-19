//
// Created by Harunobu Daikoku on 2016/08/01.
//

#ifndef SOCKET_SOCKET_CLIENT_POOL_H
#define SOCKET_SOCKET_CLIENT_POOL_H

#include <poll.h>
#include <vector>
#include <unordered_map>

#include "logger.h"
#include "socket_client.h"

#define ERR_POLL_TIMED_OUT -1
#define ERR_POLL_FAILED    -2
#define ERR_CONN_FAILED    -3

class SocketClientPool : public Logger {
 public:
  SocketClientPool(const std::string &log_tag = "SocketClientPool") : Logger(log_tag), cleanup_fds_(false) {}

  int AddClient(const std::string &addr, uint16_t port) {
    std::unique_ptr<SocketClient> client(new SocketClient(addr, port));
    int sock_fd = client->NonBlockingConnect();
    if (sock_fd < 0) {
      LogError("failed to establish connection to " + client->GetPeerNameAsString());
      return ERR_CONN_FAILED;
    }
    clients_.emplace(std::make_pair(sock_fd, std::move(client)));
    fds_.emplace_back(pollfd{sock_fd, POLLOUT, 0});

    return 0;
  }

  int Run(int timeout = 3 * 60 * 1000) {
    while (true) {
      auto num_fds = fds_.size();
      if (num_fds == 0) {
        break;
      }

      cleanup_fds_ = false;
      // default timeout: 3 mins
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
        auto &client = clients_[pfd.fd];
        auto &revents = pfd.revents;

        if (revents == 0) {
          // this file descriptor is not ready yet
          continue;
        } else if (revents & (POLLHUP | POLLERR)) {
          // the socket is hanged-up or on error
          int val = 0;
          client->GetSockOpt(SOL_SOCKET, SO_ERROR, val);
          switch (val) {
            case 0:
              // no errors observed
              break;
            case ECONNREFUSED: {
              // connection has not been established yet.
              // close connection and try again.
              LogDebug("server " + client->GetPeerNameAsString() + " is not ready yet.");
              std::string server_addr(client->GetServerAddr());
              uint16_t server_port = client->GetServerPort();
              Close(pfd);
              AddClient(server_addr, server_port);
              continue;
            }
            default:
              // unrecoverable error.
              LogError(val);
              LogError("connection to " + client->GetPeerNameAsString() + " is being closed on error");
              Close(pfd);
              return ERR_CONN_FAILED;
          }
        }

        if (revents & POLLOUT) {
          int val = 0;
          client->GetSockOpt(SOL_SOCKET, SO_ERROR, val);
          switch (val) {
            case 0:
              if (OnSend(pfd, *client)) {
                pfd.events &= ~POLLOUT;
              }
              break;
            case ECONNREFUSED: {
              // connection has not been established yet.
              // close connection and try again.
              LogDebug("server " + client->GetPeerNameAsString() + " is not ready yet.");
              std::string server_addr(client->GetServerAddr());
              uint16_t server_port = client->GetServerPort();
              Close(pfd);
              AddClient(server_addr, server_port);
              continue;
            }
            default:
              // unrecoverable error.
              LogError(val);
              LogError("connection to " + client->GetPeerNameAsString() + " is being closed on error");
              Close(pfd);
              return ERR_CONN_FAILED;
          }
        }
        if (revents & POLLIN) {
          if (OnRecv(pfd, *client, recv_buffers_[pfd.fd])) {
            pfd.events &= ~POLLIN;
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
      std::this_thread::yield();
    }

    return 0;
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
    recv_buffers_.erase(pfd.fd);
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
  std::unordered_map<int, std::unique_ptr<SocketClient>> clients_;
  std::unordered_map<int, RecvBuffer> recv_buffers_;
  std::vector<struct pollfd> fds_;
  bool cleanup_fds_;

};

#endif //SOCKET_SOCKET_CLIENT_POOL_H
