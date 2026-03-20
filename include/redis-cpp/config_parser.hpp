#pragma once

#include "redis-cpp/result.hpp"
#include "redis-cpp/server_config.hpp"

namespace redis {

class ConfigParser {
 public:
  Result<ServerConfig> Parse(int argc, char** argv) const;
};

}  // namespace redis

