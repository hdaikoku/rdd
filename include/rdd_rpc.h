//
// Created by Harunobu Daikoku on 2015/11/06.
//

#ifndef SLAVERDD_RESPONSE_H
#define SLAVERDD_RESPONSE_H

#include <msgpack.hpp>

namespace rdd_rpc {
enum class Message {
  HELLO,
  DIST_TEXT,
  MAP,
  SHUFFLE,
  REDUCE
};

enum class Response {
  OK,
  ERR,
  NO_SUCH_RDD,
  NO_SUCH_OP
};
}

MSGPACK_ADD_ENUM(rdd_rpc::Response);
MSGPACK_ADD_ENUM(rdd_rpc::Message);


#endif //SLAVERDD_RESPONSE_H
