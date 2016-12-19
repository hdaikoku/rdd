//
// Created by Harunobu Daikoku on 2016/04/04.
//

#include "worker/shuffle/fully_connected_server.h"

bool FullyConnectedServer::OnRecv(struct pollfd &pfd, const SocketCommon &socket) {
  int owner_id;
  auto recvd = socket.ReadSome(&owner_id, sizeof(owner_id));
  if (recvd < 0) {
    if (recvd == CONNECTION_ERROR) {
      // error
      std::cerr << "SERVER: ERROR" << std::endl;
      // TODO: might be better to close all connections
      // Break();
    } else if (recvd == CONNECTION_CLOSED) {
      // TODO: do something
    }

    return false;
  }

  if (recvd == sizeof(int)) {
    msgpack::sbuffer sbuf;
    std::vector<std::unique_ptr<char[]>> refs;
    auto len = block_mgr_.GroupPackBlocks(partitions_by_owner_[owner_id], sbuf, refs);

    SendBuffer header(&len, sizeof(len));
    ScheduleSend(pfd, std::move(header));
    if (len > 0) {
      SendBuffer body(std::unique_ptr<char[]>(sbuf.release()), len);
      ScheduleSend(pfd, std::move(body));
    } else if (len < 0) {
      partitions_by_owner_.erase(owner_id);
    }
  }

  return true;
}

bool FullyConnectedServer::OnSend(struct pollfd &pfd,
                                  const SocketCommon &socket,
                                  SocketNonBlockingServer::SendBuffer &send_buffer) {
  auto size = send_buffer.GetSize();
  auto sent = socket.WriteSome(send_buffer.Get(), size);
  if (sent < 0) {
    // error
    std::cerr << "SERVER could not send a block" << std::endl;
    return false;
  }

  send_buffer.Consumed(sent);

  return (sent == size);
}

bool FullyConnectedServer::IsRunning() {
  return partitions_by_owner_.size() > 0;
}

void FullyConnectedServer::Start() {
  server_thread_ = std::thread([this] {
    Run();
  });
}

void FullyConnectedServer::Stop() {
  if (server_thread_.joinable()) {
    server_thread_.join();
  }
}

void FullyConnectedServer::OnClose(struct pollfd &pfd) {

}
