#pragma once

#include <mutex>
#include <string>
#include <vector>

#include "redis-cpp/result.hpp"
#include "redis-cpp/server_config.hpp"

namespace redis {

class CommandExecutor;

Status PrepareAofStorage(const ServerConfig& config);
Status ReplayAof(const ServerConfig& config, CommandExecutor& executor);

class AofWriter {
 public:
  explicit AofWriter(ServerConfig config);

 Status Append(const std::vector<std::string>& command);

 private:
  ServerConfig config_;
  std::mutex mutex_;
};

}  // namespace redis
