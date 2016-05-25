//
// Created by Harunobu Daikoku on 2016/04/15.
//

#include "worker/shuffle/fully_connected_client.h"

bool FullyConnectedClient::OnSend(struct pollfd &pfd, SocketClient &client) {
  if (client.Write(&my_owner_id_, sizeof(my_owner_id_)) < 0) {
    std::cerr << "FC-CLIENT: failed to send fetch request" << std::endl;
    return false;
  }

  ScheduleRecv(pfd, sizeof(int));

  return true;
}

bool FullyConnectedClient::OnRecv(struct pollfd &pfd, SocketClient &client, RecvBuffer &rbuffer) {
  auto recvd = client.ReadSome(pfd.fd, rbuffer.Get(), rbuffer.GetSize());
  if (recvd < 0) {
    if (recvd == CONNECTION_ERROR) {
      // error
      // TODO: might be better to close all connections
      std::cerr << "SERVER: ERROR" << std::endl;
    } else if (recvd == CONNECTION_CLOSED) {
      // TODO: do something
    }

    return false;
  }

  rbuffer.Consumed(recvd);
  if (rbuffer.IsFull()) {
    if (rbuffer.GetCapacity() == sizeof(int)) {
      // received a header
      auto len = rbuffer.As<int>();
      if (len == -1) {
        // there's no more blocks to fetch
        Close(pfd);
      } else if (len == 0) {
        // there will be some more blocks to fetch
        backoff_voted_++;
        ScheduleSend(pfd);
      } else {
        // a block is coming
        ScheduleRecv(pfd, len);
        return false;
      }
    } else {
      // received a block
      block_mgr_.GroupUnpackBlocks(rbuffer.Data(), rbuffer.GetCapacity());
      ScheduleSend(pfd);
    }

    return true;
  }

  return false;
}

void FullyConnectedClient::Close(struct pollfd &pfd) {
  close(pfd.fd);
  pfd.fd = -1;
  num_clients_--;
}

void FullyConnectedClient::ScheduleSend(struct pollfd &pfd) {
  pfd.events |= POLLOUT;
}

void FullyConnectedClient::ScheduleRecv(struct pollfd &pfd, int32_t size) {
  recv_buffers_[pfd.fd].Reset(size);
  pfd.events |= POLLIN;
}

void FullyConnectedClient::Run() {
  // make connections to all the servers
  std::vector<struct pollfd> fds;
  for (const auto &client : clients_) {
    if (client->Connect() < 0) {
      std::cerr << "CLIENT: could not connect to one or more servers" << std::endl;
      return;
    }
    auto sockfd = client->GetSockFd();
    client->SetNonBlocking(sockfd, true);
    fds.emplace_back(pollfd{sockfd, POLLOUT | POLLHUP | POLLERR, 0});
  }

  int backoff = kMinBackoff;
  while (num_clients_ > 0) {
    // default timeout: 3 mins
    auto rc = poll(fds.data(), fds.size(), 3 * 60 * 1000);
    if (rc < 0) {
      perror("poll");
      break;
    } else if (rc == 0) {
      std::cerr << "poll timed out" << std::endl;
      break;
    }

    backoff_voted_ = 0;
    auto current_size = fds.size();
    for (int i = 0; i < current_size; i++) {
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
        if (OnSend(fds[i], *clients_[i])) {
          fds[i].events &= ~POLLOUT;
        }
      } else if (revents & POLLIN) {
        if (OnRecv(fds[i], *clients_[i], recv_buffers_[fds[i].fd])) {
          fds[i].events &= ~POLLIN;
        }
      } else {
        // ERROR
      }

      fds[i].revents = 0;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(backoff));
//    if (backoff_voted_ == num_clients_ && backoff < kMaxBackoff) {
//      TODO: proper backoff algorithm
//      backoff *= 2;
//    }
  }

}

