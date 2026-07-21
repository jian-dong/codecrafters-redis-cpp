#pragma once

#include "redis-cpp/result.hpp"
#include "redis-cpp/server_config.hpp"

namespace redis {

Status PrepareAofDirectory(const ServerConfig& config);

}  // namespace redis
