#ifndef REDIS_CPP_CONFIG_PARSER_HPP_
#define REDIS_CPP_CONFIG_PARSER_HPP_

#include "redis-cpp/result.hpp"
#include "redis-cpp/server_config.hpp"

namespace redis {

class ConfigParser {
 public:
  Result<ServerConfig> Parse(int argc, char** argv) const;
};

}  // namespace redis

#endif  // REDIS_CPP_CONFIG_PARSER_HPP_
