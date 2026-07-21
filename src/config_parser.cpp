#include "redis-cpp/config_parser.hpp"

#include <filesystem>
#include <system_error>

#include "redis-cpp/CLI11.hpp"

namespace redis {

Result<ServerConfig> ConfigParser::Parse(int argc, char** argv) const {
  ServerConfig config;
  std::error_code current_path_error;
  config.dir = std::filesystem::current_path(current_path_error).string();
  if (current_path_error) {
    return tl::make_unexpected(
        MakeCliError(CliErrorCode::kParseFailed, 1));
  }

  CLI::App app{"Redis server"};
  app.add_option("-p,--port", config.port, "TCP port to listen on")
      ->capture_default_str()
      ->check(CLI::Range(1, 65535));
  app.add_option("--backlog", config.backlog, "listen backlog")
      ->capture_default_str()
      ->check(CLI::PositiveNumber);
  app.add_option("--replicaof", config.replicaof, "master host and port");
  app.add_option("--dir", config.dir, "directory where the RDB file is stored");
  app.add_option("--dbfilename", config.dbfilename,
                 "name of the RDB file");
  app.add_option("--appendonly", config.appendonly,
                 "whether append-only persistence is enabled");
  app.add_option("--appenddirname", config.appenddirname,
                 "directory where append-only files are stored");
  app.add_option("--appendfilename", config.appendfilename,
                 "name of the append-only file");
  app.add_option("--appendfsync", config.appendfsync,
                 "append-only file synchronization policy");

  try {
    app.parse(argc, argv);
  } catch (const CLI::ParseError& error) {
    return tl::make_unexpected(
        MakeCliError(CliErrorCode::kParseFailed, app.exit(error)));
  }

  return config;
}

}  // namespace redis
