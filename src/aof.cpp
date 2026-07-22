#include "redis-cpp/aof.hpp"

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <string_view>
#include <system_error>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "redis-cpp/resp.hpp"

namespace redis {
namespace {

bool IsSafeFilename(const std::string& value) {
  const std::filesystem::path path(value);
  return !value.empty() && value != "." && value != ".." &&
         path.is_relative() && path.filename() == path;
}

Status WriteAll(int descriptor, std::string_view contents,
                const std::filesystem::path& path) {
  size_t written = 0;
  while (written < contents.size()) {
    const ssize_t result =
        write(descriptor, contents.data() + written, contents.size() - written);
    if (result < 0 && errno == EINTR) {
      continue;
    }
    if (result <= 0) {
      return tl::make_unexpected(MakeFileSystemError(
          FileSystemErrorCode::kWriteFileFailed, path.string()));
    }
    written += static_cast<size_t>(result);
  }
  return {};
}

Status Sync(int descriptor, const std::filesystem::path& path) {
  int result = 0;
  do {
    result = fsync(descriptor);
  } while (result != 0 && errno == EINTR);
  if (result != 0) {
    return tl::make_unexpected(MakeFileSystemError(
        FileSystemErrorCode::kSyncFileFailed, path.string()));
  }
  return {};
}

}  // namespace

AppendOnlyLogConfig MakeAppendOnlyLogConfig(const ServerConfig& config) {
  return AppendOnlyLogConfig{
      .enabled = config.appendonly == "yes",
      .directory =
          std::filesystem::path(config.dir) / config.appenddirname,
      .base_filename = config.appendfilename,
      .fsync_policy = config.appendfsync == "always"
                          ? AppendFsyncPolicy::kAlways
                          : AppendFsyncPolicy::kEverySecond,
  };
}

AppendOnlyLog::AppendOnlyLog(AppendOnlyLogConfig config)
    : config_(std::move(config)),
      manifest_path_(config_.directory /
                     (config_.base_filename + ".manifest")) {}

Status AppendOnlyLog::Open() {
  if (!config_.enabled) {
    return {};
  }

  std::lock_guard<std::mutex> lock(mutex_);
  if (descriptor_.IsValid()) {
    return {};
  }
  if (!IsSafeFilename(config_.base_filename)) {
    return tl::make_unexpected(MakeFileSystemError(
        FileSystemErrorCode::kInvalidPath, config_.base_filename));
  }

  std::error_code create_error;
  std::filesystem::create_directories(config_.directory, create_error);
  if (create_error) {
    return tl::make_unexpected(MakeFileSystemError(
        FileSystemErrorCode::kCreateDirectoryFailed,
        config_.directory.string()));
  }

  std::error_code manifest_status_error;
  const std::filesystem::file_status manifest_status =
      std::filesystem::symlink_status(manifest_path_, manifest_status_error);
  const bool manifest_missing =
      manifest_status.type() == std::filesystem::file_type::not_found ||
      manifest_status_error ==
          std::make_error_code(std::errc::no_such_file_or_directory);
  if (manifest_status_error && !manifest_missing) {
    return tl::make_unexpected(MakeFileSystemError(
        FileSystemErrorCode::kReadFileFailed, manifest_path_.string()));
  }
  if (!manifest_missing) {
    if (manifest_status.type() != std::filesystem::file_type::regular) {
      return tl::make_unexpected(MakeFileSystemError(
          FileSystemErrorCode::kCreateFileFailed, manifest_path_.string()));
    }
  } else {
    Status create_status = CreateInitialFiles();
    if (!create_status) {
      return create_status;
    }
  }

  Result<std::string> incremental_file = FindIncrementalFile();
  if (!incremental_file) {
    return tl::make_unexpected(incremental_file.error());
  }
  active_file_path_ = config_.directory / *incremental_file;

  const int descriptor = open(active_file_path_.c_str(),
                              O_RDWR | O_APPEND | O_CLOEXEC | O_NOFOLLOW);
  if (descriptor < 0) {
    return tl::make_unexpected(MakeFileSystemError(
        FileSystemErrorCode::kWriteFileFailed, active_file_path_.string()));
  }
  struct stat descriptor_status {};
  if (fstat(descriptor, &descriptor_status) != 0 ||
      !S_ISREG(descriptor_status.st_mode)) {
    close(descriptor);
    return tl::make_unexpected(MakeFileSystemError(
        FileSystemErrorCode::kInvalidPath, active_file_path_.string()));
  }
  descriptor_.Reset(descriptor);
  return {};
}

Status AppendOnlyLog::CreateInitialFiles() {
  const std::filesystem::path append_file_path =
      config_.directory / (config_.base_filename + ".1.incr.aof");
  UniqueFd append_file(open(append_file_path.c_str(),
                            O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC |
                                O_NOFOLLOW,
                            0644));
  if (!append_file.IsValid()) {
    return tl::make_unexpected(MakeFileSystemError(
        FileSystemErrorCode::kCreateFileFailed, append_file_path.string()));
  }
  struct stat append_file_status {};
  if (fstat(append_file.Get(), &append_file_status) != 0 ||
      !S_ISREG(append_file_status.st_mode)) {
    return tl::make_unexpected(MakeFileSystemError(
        FileSystemErrorCode::kInvalidPath, append_file_path.string()));
  }

  std::string temporary_template =
      (manifest_path_.string() + ".tmp.XXXXXX");
  std::vector<char> temporary_path(temporary_template.begin(),
                                   temporary_template.end());
  temporary_path.push_back('\0');
  UniqueFd manifest_file(mkstemp(temporary_path.data()));
  if (!manifest_file.IsValid()) {
    return tl::make_unexpected(MakeFileSystemError(
        FileSystemErrorCode::kCreateFileFailed, manifest_path_.string()));
  }
  const std::filesystem::path temporary_manifest(temporary_path.data());
  const std::string manifest_contents =
      "file " + append_file_path.filename().string() + " seq 1 type i\n";
  Status write_status =
      WriteAll(manifest_file.Get(), manifest_contents, temporary_manifest);
  if (!write_status) {
    (void)unlink(temporary_manifest.c_str());
    return write_status;
  }
  Status sync_status = Sync(manifest_file.Get(), temporary_manifest);
  if (!sync_status) {
    (void)unlink(temporary_manifest.c_str());
    return sync_status;
  }
  if (rename(temporary_manifest.c_str(), manifest_path_.c_str()) != 0) {
    (void)unlink(temporary_manifest.c_str());
    return tl::make_unexpected(MakeFileSystemError(
        FileSystemErrorCode::kCreateFileFailed, manifest_path_.string()));
  }
  return {};
}

Result<std::string> AppendOnlyLog::FindIncrementalFile() const {
  std::ifstream manifest(manifest_path_);
  if (!manifest.is_open()) {
    return tl::make_unexpected(MakeFileSystemError(
        FileSystemErrorCode::kReadFileFailed, manifest_path_.string()));
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
        IsSafeFilename(file_name)) {
      return file_name;
    }
  }

  if (!manifest.eof()) {
    return tl::make_unexpected(MakeFileSystemError(
        FileSystemErrorCode::kReadFileFailed, manifest_path_.string()));
  }
  return tl::make_unexpected(MakeFileSystemError(
      FileSystemErrorCode::kInvalidFileFormat, manifest_path_.string()));
}

Status AppendOnlyLog::Append(const std::vector<std::string>& command) {
  return AppendEncoded(RespWriter::WriteCommand(command));
}

Status AppendOnlyLog::AppendTransaction(
    const std::vector<std::vector<std::string>>& commands) {
  if (commands.empty()) {
    return {};
  }

  std::string encoded = RespWriter::WriteCommand({"MULTI"});
  for (const std::vector<std::string>& command : commands) {
    encoded += RespWriter::WriteCommand(command);
  }
  encoded += RespWriter::WriteCommand({"EXEC"});
  return AppendEncoded(std::move(encoded));
}

Status AppendOnlyLog::AppendEncoded(std::string encoded) {
  if (!config_.enabled) {
    return {};
  }
  Status open_status = Open();
  if (!open_status) {
    return open_status;
  }

  std::lock_guard<std::mutex> lock(mutex_);
  Status write_status =
      WriteAll(descriptor_.Get(), encoded, active_file_path_);
  if (!write_status) {
    return write_status;
  }

  if (config_.fsync_policy == AppendFsyncPolicy::kAlways) {
    return Sync(descriptor_.Get(), active_file_path_);
  }
  return {};
}

Status AppendOnlyLog::Replay(const ReplayCommand& execute) {
  if (!config_.enabled) {
    return {};
  }
  Status open_status = Open();
  if (!open_status) {
    return open_status;
  }

  Result<std::string> contents = ReadContents();
  if (!contents) {
    return tl::make_unexpected(contents.error());
  }

  RespParser parser(RespParser::InputMode::kRespOnly);
  parser.Append(*contents);
  bool in_transaction = false;
  std::vector<std::vector<std::string>> transaction;
  while (parser.BufferSize() > 0) {
    Result<std::optional<std::vector<std::string>>> command =
        parser.NextCommand();
    if (!command || !command->has_value()) {
      return tl::make_unexpected(MakeFileSystemError(
          FileSystemErrorCode::kInvalidFileFormat,
          active_file_path_.string()));
    }
    const std::string command_name =
        command->value().empty() ? "" : ToUpperAscii(command->value()[0]);
    if (command_name == "MULTI") {
      if (in_transaction) {
        return tl::make_unexpected(MakeFileSystemError(
            FileSystemErrorCode::kInvalidFileFormat,
            active_file_path_.string()));
      }
      in_transaction = true;
      transaction.push_back(std::move(**command));
      continue;
    }
    if (in_transaction) {
      transaction.push_back(std::move(**command));
      if (command_name != "EXEC") {
        continue;
      }

      for (const std::vector<std::string>& transaction_command : transaction) {
        Status execute_status = execute(transaction_command);
        if (!execute_status) {
          return tl::make_unexpected(MakeFileSystemError(
              FileSystemErrorCode::kInvalidFileFormat,
              active_file_path_.string()));
        }
      }
      transaction.clear();
      in_transaction = false;
      continue;
    }

    Status execute_status = execute(**command);
    if (!execute_status) {
      return tl::make_unexpected(MakeFileSystemError(
          FileSystemErrorCode::kInvalidFileFormat,
          active_file_path_.string()));
    }
  }
  if (in_transaction) {
    return tl::make_unexpected(MakeFileSystemError(
        FileSystemErrorCode::kInvalidFileFormat,
        active_file_path_.string()));
  }
  return {};
}

Result<std::string> AppendOnlyLog::ReadContents() {
  std::lock_guard<std::mutex> lock(mutex_);
  struct stat status {};
  if (fstat(descriptor_.Get(), &status) != 0 || !S_ISREG(status.st_mode) ||
      status.st_size < 0) {
    return tl::make_unexpected(MakeFileSystemError(
        FileSystemErrorCode::kReadFileFailed, active_file_path_.string()));
  }

  std::string contents(static_cast<size_t>(status.st_size), '\0');
  size_t read_size = 0;
  while (read_size < contents.size()) {
    const ssize_t result = pread(descriptor_.Get(), contents.data() + read_size,
                                 contents.size() - read_size,
                                 static_cast<off_t>(read_size));
    if (result < 0 && errno == EINTR) {
      continue;
    }
    if (result <= 0) {
      return tl::make_unexpected(MakeFileSystemError(
          FileSystemErrorCode::kReadFileFailed, active_file_path_.string()));
    }
    read_size += static_cast<size_t>(result);
  }
  return contents;
}

}  // namespace redis
