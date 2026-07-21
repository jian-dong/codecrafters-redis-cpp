#include "redis-cpp/aof.hpp"

#include <cerrno>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <sstream>
#include <system_error>

#include <fcntl.h>
#include <unistd.h>

#include "redis-cpp/command_executor.hpp"
#include "redis-cpp/resp.hpp"

namespace redis {
namespace {

std::filesystem::path AppendDirectory(const ServerConfig& config) {
  return std::filesystem::path(config.dir) / config.appenddirname;
}

std::filesystem::path ManifestPath(const ServerConfig& config) {
  return AppendDirectory(config) / (config.appendfilename + ".manifest");
}

Result<std::string> FindIncrementalFile(const ServerConfig& config) {
  const std::filesystem::path manifest_path = ManifestPath(config);
  std::ifstream manifest(manifest_path);
  if (!manifest.is_open()) {
    return tl::make_unexpected(MakeFileSystemError(
        FileSystemErrorCode::kReadFileFailed, manifest_path.string()));
  }

  std::string line;
  while (std::getline(manifest, line)) {
    std::istringstream fields(line);
    std::string file_label;
    std::string file_name;
    std::string sequence_label;
    std::string sequence;
    std::string type_label;
    std::string type;
    std::string extra;
    if (fields >> file_label >> file_name >> sequence_label >> sequence >>
            type_label >> type &&
        !(fields >> extra) && file_label == "file" &&
        sequence_label == "seq" && type_label == "type" && type == "i" &&
        std::filesystem::path(file_name).filename() == file_name) {
      return file_name;
    }
  }

  if (!manifest.eof()) {
    return tl::make_unexpected(MakeFileSystemError(
        FileSystemErrorCode::kReadFileFailed, manifest_path.string()));
  }
  return tl::make_unexpected(MakeFileSystemError(
      FileSystemErrorCode::kInvalidFileFormat, manifest_path.string()));
}

}  // namespace

Status PrepareAofStorage(const ServerConfig& config) {
  if (config.appendonly != "yes") {
    return {};
  }

  const std::filesystem::path append_directory = AppendDirectory(config);
  std::error_code create_error;
  std::filesystem::create_directories(append_directory, create_error);
  if (create_error) {
    return tl::make_unexpected(MakeFileSystemError(
        FileSystemErrorCode::kCreateDirectoryFailed,
        append_directory.string()));
  }

  const std::filesystem::path manifest_path = ManifestPath(config);
  std::error_code manifest_status_error;
  const bool manifest_exists =
      std::filesystem::exists(manifest_path, manifest_status_error);
  if (manifest_status_error) {
    return tl::make_unexpected(MakeFileSystemError(
        FileSystemErrorCode::kReadFileFailed, manifest_path.string()));
  }
  if (manifest_exists) {
    std::error_code file_type_error;
    const bool is_regular_file =
        std::filesystem::is_regular_file(manifest_path, file_type_error);
    if (file_type_error || !is_regular_file) {
      return tl::make_unexpected(MakeFileSystemError(
          FileSystemErrorCode::kCreateFileFailed, manifest_path.string()));
    }
    return {};
  }

  const std::filesystem::path append_file_path =
      append_directory / (config.appendfilename + ".1.incr.aof");
  std::ofstream append_file(append_file_path,
                            std::ios::binary | std::ios::app);
  if (!append_file.is_open()) {
    return tl::make_unexpected(MakeFileSystemError(
        FileSystemErrorCode::kCreateFileFailed, append_file_path.string()));
  }

  std::ofstream manifest_file(manifest_path, std::ios::binary);
  if (!manifest_file.is_open()) {
    return tl::make_unexpected(MakeFileSystemError(
        FileSystemErrorCode::kCreateFileFailed, manifest_path.string()));
  }

  manifest_file << "file " << append_file_path.filename().string()
                << " seq 1 type i\n";
  manifest_file.flush();
  if (!manifest_file) {
    return tl::make_unexpected(MakeFileSystemError(
        FileSystemErrorCode::kWriteFileFailed, manifest_path.string()));
  }

  return {};
}

AofWriter::AofWriter(ServerConfig config) : config_(std::move(config)) {}

Status AofWriter::Append(const std::vector<std::string>& command) {
  if (config_.appendonly != "yes") {
    return {};
  }

  std::lock_guard<std::mutex> lock(mutex_);
  Result<std::string> incremental_file = FindIncrementalFile(config_);
  if (!incremental_file) {
    return tl::make_unexpected(incremental_file.error());
  }

  const std::filesystem::path append_file_path =
      AppendDirectory(config_) / *incremental_file;
  const int descriptor =
      open(append_file_path.c_str(), O_WRONLY | O_APPEND | O_CLOEXEC);
  if (descriptor < 0) {
    return tl::make_unexpected(MakeFileSystemError(
        FileSystemErrorCode::kWriteFileFailed, append_file_path.string()));
  }

  const std::string encoded = RespWriter::Write(RespArray{command});
  size_t written = 0;
  while (written < encoded.size()) {
    const ssize_t result =
        write(descriptor, encoded.data() + written, encoded.size() - written);
    if (result < 0 && errno == EINTR) {
      continue;
    }
    if (result <= 0) {
      close(descriptor);
      return tl::make_unexpected(MakeFileSystemError(
          FileSystemErrorCode::kWriteFileFailed, append_file_path.string()));
    }
    written += static_cast<size_t>(result);
  }

  if (config_.appendfsync == "always" && fsync(descriptor) != 0) {
    close(descriptor);
    return tl::make_unexpected(MakeFileSystemError(
        FileSystemErrorCode::kSyncFileFailed, append_file_path.string()));
  }
  if (close(descriptor) != 0) {
    return tl::make_unexpected(MakeFileSystemError(
        FileSystemErrorCode::kWriteFileFailed, append_file_path.string()));
  }

  return {};
}

Status ReplayAof(const ServerConfig& config, CommandExecutor& executor) {
  if (config.appendonly != "yes") {
    return {};
  }

  Result<std::string> incremental_file = FindIncrementalFile(config);
  if (!incremental_file) {
    return tl::make_unexpected(incremental_file.error());
  }

  const std::filesystem::path append_file_path =
      AppendDirectory(config) / *incremental_file;
  std::ifstream append_file(append_file_path, std::ios::binary);
  if (!append_file.is_open()) {
    return tl::make_unexpected(MakeFileSystemError(
        FileSystemErrorCode::kReadFileFailed, append_file_path.string()));
  }

  const std::string encoded{
      std::istreambuf_iterator<char>(append_file),
      std::istreambuf_iterator<char>()};
  if (append_file.bad()) {
    return tl::make_unexpected(MakeFileSystemError(
        FileSystemErrorCode::kReadFileFailed, append_file_path.string()));
  }

  RespParser parser;
  parser.Append(encoded);
  while (parser.BufferSize() > 0) {
    Result<std::optional<std::vector<std::string>>> command =
        parser.NextCommand();
    if (!command || !command->has_value()) {
      return tl::make_unexpected(MakeFileSystemError(
          FileSystemErrorCode::kInvalidFileFormat,
          append_file_path.string()));
    }

    CommandResult result = executor.Execute(**command);
    if (!result) {
      return tl::make_unexpected(MakeFileSystemError(
          FileSystemErrorCode::kInvalidFileFormat,
          append_file_path.string()));
    }
  }

  return {};
}

}  // namespace redis
