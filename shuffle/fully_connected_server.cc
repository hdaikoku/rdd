//
// Created by Harunobu Daikoku on 2016/04/04.
//

#include "shuffle/fully_connected_server.h"

bool FullyConnectedServer::OnRecv(struct pollfd &pfd) {
  auto fd = pfd.fd;

  int partition_id;
  auto recvd = ReadSome(fd, &partition_id, sizeof(partition_id));
  if (recvd < 0) {
    if (recvd == CONNECTION_ERROR) {
      // error
      std::cerr << "SERVER: ERROR" << std::endl;
    }
    Break();
    return false;
  }

  if (recvd == sizeof(int)) {
    if (partition_id == -1) {
      num_completed_++;
      SendBuffer header(&partition_id, sizeof(partition_id));
      ScheduleSend(pfd, std::move(header));
      return true;
    }
    int32_t len;
    auto block = block_mgr_.GetBlock(partition_id, len);

    SendBuffer header(&len, sizeof(len));
    ScheduleSend(pfd, std::move(header));
    if (len > 0) {
      SendBuffer body(std::move(block), len);
      ScheduleSend(pfd, std::move(body));
    }
  }

  return true;
}

bool FullyConnectedServer::OnSend(struct pollfd &pfd, SendBuffer &send_buffer) {
  auto size = send_buffer.GetSize();
  auto sent = WriteSome(pfd.fd, send_buffer.Get(), size);
  if (sent < 0) {
    // error
    std::cerr << "SERVER could not send a block" << std::endl;
    return false;
  }

  send_buffer.Consumed(sent);

  return (sent == size);
}

bool FullyConnectedServer::IsRunning() {
  return (num_completed_ < num_clients_);
}