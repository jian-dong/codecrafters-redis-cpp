#pragma once

#include "redis-cpp/command_processor.hpp"
#include "redis-cpp/resp.hpp"
#include "redis-cpp/socket.hpp"

namespace redis {

class ClientSession {
 public:
  ClientSession(Socket socket, CommandProcessor& command_processor);

  void Run();

 private:
  Socket socket_;
  CommandProcessor& command_processor_;
  RespParser parser_;
  bool in_multi_ = false;
};

}  // namespace redis

