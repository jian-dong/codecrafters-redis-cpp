#include "redis-cpp/application.hpp"

#include <iostream>

#include "redis-cpp/redis_server.hpp"

namespace redis {

int Application::Run(int argc, char** argv) {
  Result<ServerConfig> config = config_parser_.Parse(argc, argv);
  if (!config) {
    return ExitWithError(config.error());
  }

  RedisServer server(*config);
  Status run_status = server.Run();
  if (!run_status) {
    return ExitWithError(run_status.error());
  }

  return 0;
}

int Application::ExitWithError(const Error& error) {
  if (error.ShouldLog()) {
    std::cerr << error.Message() << "\n";
  }
  return error.ExitCode();
}

}  // namespace redis
