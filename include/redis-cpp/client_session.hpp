#ifndef REDIS_CPP_CLIENT_SESSION_HPP_
#define REDIS_CPP_CLIENT_SESSION_HPP_

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
};

}  // namespace redis

#endif  // REDIS_CPP_CLIENT_SESSION_HPP_
