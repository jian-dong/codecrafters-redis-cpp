#pragma once

#include "redis-cpp/command_processor.hpp"
#include "redis-cpp/replica_manager.hpp"
#include "redis-cpp/resp.hpp"
#include "redis-cpp/socket.hpp"

#include <unordered_set>

namespace redis {

class ClientSession {
 public:
  ClientSession(Socket socket, CommandProcessor& command_processor,
                ReplicaManager* replica_manager = nullptr);

  void Run();

 private:
  Socket socket_;
  CommandProcessor& command_processor_;
  ReplicaManager* replica_manager_;
  RespParser parser_;
  bool in_multi_ = false;
  std::vector<std::vector<std::string>> queued_commands_;
  std::unordered_set<std::string> subscribed_channels_;
  std::unordered_set<std::string> subscribed_patterns_;
};

}  // namespace redis
