#pragma once

#include "redis-cpp/database.hpp"
#include "redis-cpp/server_config.hpp"

namespace redis {

void LoadDatabaseFromRdb(const ServerConfig& config, Database& database);

}  // namespace redis
