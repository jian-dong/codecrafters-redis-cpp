#pragma once

#include "redis-cpp/command_executor.hpp"
#include "redis-cpp/pubsub_manager.hpp"
#include "redis-cpp/replica_manager.hpp"
#include "redis-cpp/resp.hpp"
#include "redis-cpp/socket.hpp"

#include <unordered_set>

namespace redis {

class ClientSession {
 public:
  ClientSession(Socket socket, CommandExecutor& command_executor,
                ReplicaManager* replica_manager = nullptr,
                PubSubManager* pubsub_manager = nullptr);
  ~ClientSession();

  void Run();

 private:
  bool SendResponse(const std::string& response);

  Socket socket_;
  CommandExecutor& command_executor_;
  ReplicaManager* replica_manager_;
  PubSubManager* pubsub_manager_;
  RespParser parser_;
  bool authenticated_ = true;
  bool in_multi_ = false;
  std::vector<std::vector<std::string>> queued_commands_;
  std::unordered_set<std::string> subscribed_channels_;
  std::unordered_set<std::string> subscribed_patterns_;
};

}  // namespace redis
