//
// Created by Harunobu Daikoku on 2016/04/15.
//

#include "worker/shuffle/fully_connected_client.h"

bool FullyConnectedClient::OnRecv(struct pollfd &pfd,
                                  const SocketCommon &socket,
                                  SocketClientPool::RecvBuffer &rbuffer) {

  auto recvd = socket.ReadSome(rbuffer.Get(), rbuffer.GetSize());
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
  if (rbuffer.GetSize() == 0) {
    if (rbuffer.GetTag() == kTagHeader) {
      // received a header
      auto len = rbuffer.As<int>();
      if (len == -1) {
        // there's no more blocks to fetch
        Close(pfd);
      } else if (len == 0) {
        // there will be some more blocks to fetch
        ScheduleSend(pfd);
      } else {
        // a block is coming
        ScheduleRecv(pfd, len, kTagBody);
        return false;
      }
    } else if (rbuffer.GetTag() == kTagBody) {
      // received a block
      block_mgr_.GroupUnpackBlocks(rbuffer.Data(), rbuffer.GetCapacity());
      ScheduleSend(pfd);
    }

    return true;
  }

  return false;
}

bool FullyConnectedClient::OnSend(struct pollfd &pfd, const SocketCommon &socket) {
  if (socket.Write(&my_owner_id_, sizeof(my_owner_id_)) < 0) {
    std::cerr << "FullyConnectedClient: failed to send fetch request" << std::endl;
    return false;
  }

  ScheduleRecv(pfd, sizeof(int), kTagHeader);

  return true;
}
