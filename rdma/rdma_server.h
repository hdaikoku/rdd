//
// Created by hdaikoku on 15/11/10.
//

#ifndef RDMA_SERVER_CLIENT_RDMA_SERVER_H
#define RDMA_SERVER_CLIENT_RDMA_SERVER_H

#include <string>
#include "rdma_common.h"

class RDMAServer: public RDMACommon {
 public:

  RDMAServer(const std::string &server_port) : server_port_(server_port) { }

  bool Listen();

  int Accept();

  virtual bool SetSockOpt() override;

 private:
  std::string server_port_;
};


#endif //RDMA_SERVER_CLIENT_RDMA_SERVER_H
