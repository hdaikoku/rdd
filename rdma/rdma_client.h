//
// Created by hdaikoku on 15/11/10.
//

#ifndef RDMA_SERVER_CLIENT_RDMA_CLIENT_H
#define RDMA_SERVER_CLIENT_RDMA_CLIENT_H

#include <string>
#include "rdma_common.h"

class RDMAClient: public RDMACommon {
 public:

  RDMAClient(const std::string &server_addr, const std::string &server_port)
      : server_addr_(server_addr), server_port_(server_port) { }

  int Connect();

 private:
  std::string server_addr_;
  std::string server_port_;
};


#endif //RDMA_SERVER_CLIENT_RDMA_CLIENT_H
