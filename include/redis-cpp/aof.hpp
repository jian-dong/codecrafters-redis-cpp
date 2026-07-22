#pragma once

#include <filesystem>
#include <functional>
#include <mutex>
#include <string>
#include <vector>

#include "redis-cpp/result.hpp"
#include "redis-cpp/server_config.hpp"
#include "redis-cpp/unique_fd.hpp"

namespace redis {

enum class AppendFsyncPolicy {
  kEverySecond,
  kAlways,
};

struct AppendOnlyLogConfig {
  bool enabled = false;
  std::filesystem::path directory;
  std::string base_filename = "appendonly.aof";
  AppendFsyncPolicy fsync_policy = AppendFsyncPolicy::kEverySecond;
};

AppendOnlyLogConfig MakeAppendOnlyLogConfig(const ServerConfig& config);

class AppendOnlyLog {
 public:
  using ReplayCommand =
      std::function<Status(const std::vector<std::string>&)>;

  explicit AppendOnlyLog(AppendOnlyLogConfig config);

  Status Open();
  Status Append(const std::vector<std::string>& command);
  Status AppendTransaction(
      const std::vector<std::vector<std::string>>& commands);
  Status Replay(const ReplayCommand& execute);

 private:
  Status AppendEncoded(std::string encoded);
  Result<std::string> ReadContents();
  Result<std::string> FindIncrementalFile() const;
  Status CreateInitialFiles();

  AppendOnlyLogConfig config_;
  std::filesystem::path manifest_path_;
  std::filesystem::path active_file_path_;
  UniqueFd descriptor_;
  std::mutex mutex_;
};

}  // namespace redis
