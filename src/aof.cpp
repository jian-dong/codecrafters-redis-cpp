#include "redis-cpp/aof.hpp"

#include <filesystem>
#include <system_error>

namespace redis {

Status PrepareAofDirectory(const ServerConfig& config) {
  if (config.appendonly != "yes") {
    return {};
  }

  const std::filesystem::path append_directory =
      std::filesystem::path(config.dir) / config.appenddirname;
  std::error_code create_error;
  std::filesystem::create_directories(append_directory, create_error);
  if (create_error) {
    return tl::make_unexpected(MakeFileSystemError(
        FileSystemErrorCode::kCreateDirectoryFailed,
        append_directory.string()));
  }

  return {};
}

}  // namespace redis
