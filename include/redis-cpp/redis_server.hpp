#pragma once

#include "redis-cpp/aof.hpp"
#include "redis-cpp/command_executor.hpp"
#include "redis-cpp/database.hpp"
#include "redis-cpp/pubsub_manager.hpp"
#include "redis-cpp/replica_manager.hpp"
#include "redis-cpp/replication_client.hpp"
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

  ServerConfig config_;
  AppendOnlyLog append_only_log_;
  Database database_;
  PubSubManager pubsub_manager_;
  ReplicaManager replica_manager_;
  CommandExecutor command_executor_;
  ReplicationClient replication_client_;
};

}  // namespace redis
