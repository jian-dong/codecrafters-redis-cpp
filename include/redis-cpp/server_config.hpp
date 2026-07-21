#pragma once

#include <string>

namespace redis {

struct ServerConfig {
  int port = 6379;
  int backlog = 5;
  std::string replicaof;  // empty = master, "host port" = replica
  std::string dir;
  std::string dbfilename;
  std::string appendonly = "no";
  std::string appenddirname = "appendonlydir";
  std::string appendfilename = "appendonly.aof";
  std::string appendfsync = "everysec";
};

}  // namespace redis
