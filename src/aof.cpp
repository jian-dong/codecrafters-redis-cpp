#include "redis-cpp/aof.hpp"

#include <filesystem>
#include <fstream>
#include <system_error>

namespace redis {

Status PrepareAofStorage(const ServerConfig& config) {
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

  const std::filesystem::path append_file_path =
      append_directory / (config.appendfilename + ".1.incr.aof");
  std::ofstream append_file(append_file_path,
                            std::ios::binary | std::ios::app);
  if (!append_file.is_open()) {
    return tl::make_unexpected(MakeFileSystemError(
        FileSystemErrorCode::kCreateFileFailed, append_file_path.string()));
  }

  return {};
}

}  // namespace redis
