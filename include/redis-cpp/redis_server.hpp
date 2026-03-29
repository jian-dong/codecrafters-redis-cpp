#pragma once

#include "redis-cpp/command_processor.hpp"
#include "redis-cpp/database.hpp"
#include "redis-cpp/pubsub_manager.hpp"
#include "redis-cpp/replica_manager.hpp"
#include "redis-cpp/result.hpp"
#include "redis-cpp/server_config.hpp"
#include "redis-cpp/socket.hpp"

namespace redis {

class RedisServer {
 public:
  explicit RedisServer(ServerConfig config);

  Status Run();

 private:
  void ServeClient(Socket socket);
  Status ConnectToMaster();
  void ProcessReplicatedCommands();

  ServerConfig config_;
  Database database_;
  PubSubManager pubsub_manager_;
  ReplicaManager replica_manager_;
  CommandProcessor command_processor_;
  Socket master_socket_;
  std::string master_leftover_;
};

}  // namespace redis
