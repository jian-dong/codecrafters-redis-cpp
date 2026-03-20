#ifndef REDIS_CPP_SERVER_CONFIG_HPP_
#define REDIS_CPP_SERVER_CONFIG_HPP_

namespace redis {

struct ServerConfig {
  int port = 6379;
  int backlog = 5;
};

}  // namespace redis

#endif  // REDIS_CPP_SERVER_CONFIG_HPP_
