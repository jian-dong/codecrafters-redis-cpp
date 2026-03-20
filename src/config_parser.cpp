#include "redis-cpp/config_parser.hpp"

#include "redis-cpp/CLI11.hpp"

namespace redis {

Result<ServerConfig> ConfigParser::Parse(int argc, char** argv) const {
  ServerConfig config;
  CLI::App app{"Redis server"};
  app.add_option("-p,--port", config.port, "TCP port to listen on")
      ->capture_default_str()
      ->check(CLI::Range(1, 65535));
  app.add_option("--backlog", config.backlog, "listen backlog")
      ->capture_default_str()
      ->check(CLI::PositiveNumber);

  try {
    app.parse(argc, argv);
  } catch (const CLI::ParseError& error) {
    return tl::make_unexpected(
        MakeCliError(CliErrorCode::kParseFailed, app.exit(error)));
  }

  return config;
}

}  // namespace redis
