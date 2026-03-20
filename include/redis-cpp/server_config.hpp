#pragma once

namespace redis {

struct ServerConfig {
  int port = 6379;
  int backlog = 5;
};

}  // namespace redis

