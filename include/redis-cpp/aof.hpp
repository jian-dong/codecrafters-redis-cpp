#pragma once

#include <mutex>
#include <string>
#include <vector>

#include "redis-cpp/result.hpp"
#include "redis-cpp/server_config.hpp"

namespace redis {

Status PrepareAofStorage(const ServerConfig& config);

class AofWriter {
 public:
  explicit AofWriter(ServerConfig config);

  Status Append(const std::vector<std::string>& command);

 private:
  Result<std::string> FindIncrementalFile() const;

  ServerConfig config_;
  std::mutex mutex_;
};

}  // namespace redis
