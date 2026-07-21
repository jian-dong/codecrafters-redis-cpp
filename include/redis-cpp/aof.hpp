#pragma once

#include "redis-cpp/result.hpp"
#include "redis-cpp/server_config.hpp"

namespace redis {

Status PrepareAofStorage(const ServerConfig& config);

}  // namespace redis
