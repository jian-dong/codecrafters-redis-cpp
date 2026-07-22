#pragma once

#include "redis-cpp/database.hpp"
#include "redis-cpp/result.hpp"
#include "redis-cpp/server_config.hpp"

namespace redis {

Status LoadDatabaseFromRdb(const ServerConfig& config, Database& database);

}  // namespace redis
