#pragma once

#include <string>

#include "redis-cpp/command_executor.hpp"
#include "redis-cpp/result.hpp"
#include "redis-cpp/socket.hpp"

namespace redis {

class ReplicationClient {
 public:
  explicit ReplicationClient(CommandExecutor& executor)
      : executor_(executor) {}

  Status Connect(std::string master_endpoint, int listening_port);
  Status Run();

 private:
  CommandExecutor& executor_;
  Socket master_socket_;
  std::string buffered_commands_;
};

}  // namespace redis
