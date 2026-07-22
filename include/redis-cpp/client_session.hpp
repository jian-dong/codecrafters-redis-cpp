#pragma once

#include "redis-cpp/command_executor.hpp"
#include "redis-cpp/pubsub_manager.hpp"
#include "redis-cpp/replica_manager.hpp"
#include "redis-cpp/resp.hpp"
#include "redis-cpp/socket.hpp"
#include "redis-cpp/subscription_session.hpp"
#include "redis-cpp/transaction.hpp"

namespace redis {

class ClientSession {
 public:
  ClientSession(Socket socket, CommandExecutor& command_executor,
                ReplicaManager* replica_manager = nullptr,
                PubSubManager* pubsub_manager = nullptr);
  ~ClientSession();

  Status Run();

 private:
  Status SendResponse(const std::string& response);

  Socket socket_;
  SharedConnectionWriter writer_;
  CommandExecutor& command_executor_;
  ReplicaManager* replica_manager_;
  RespParser parser_;
  Transaction transaction_;
  SubscriptionSession subscription_;
  bool authenticated_ = true;
  bool replica_registered_ = false;
};

}  // namespace redis
