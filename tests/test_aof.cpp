#include "test_support.hpp"

#include <cstring>

#include "redis-cpp/aof.hpp"

namespace {

std::filesystem::path MakeTempDirectory(const char* pattern) {
  std::vector<char> writable(pattern, pattern + std::strlen(pattern) + 1);
  char* directory = mkdtemp(writable.data());
  EXPECT_NE(directory, nullptr);
  return directory == nullptr ? std::filesystem::path{} : directory;
}

redis::AppendOnlyLogConfig EnabledConfig(
    const std::filesystem::path& directory) {
  return redis::AppendOnlyLogConfig{
      .enabled = true,
      .directory = directory / "appendonlydir",
      .base_filename = "configured.aof",
      .fsync_policy = redis::AppendFsyncPolicy::kAlways,
  };
}

std::string ReadFile(const std::filesystem::path& path) {
  std::ifstream input(path, std::ios::binary);
  return {std::istreambuf_iterator<char>(input),
          std::istreambuf_iterator<char>()};
}

const redis::FileSystemError* FileError(const redis::Status& status) {
  if (status.has_value()) {
    return nullptr;
  }
  return std::get_if<redis::FileSystemError>(&status.error().Kind());
}

TEST(AppendOnlyLogTest, DisabledLogHasNoFilesystemSideEffects) {
  const std::filesystem::path parent =
      MakeTempDirectory("/tmp/redis-aof-disabledXXXXXX");
  const std::filesystem::path append_directory = parent / "disabled";
  redis::AppendOnlyLog log(redis::AppendOnlyLogConfig{
      .enabled = false, .directory = append_directory});

  EXPECT_TRUE(log.Open().has_value());
  EXPECT_TRUE(log.Append({"SET", "foo", "bar"}).has_value());
  EXPECT_FALSE(std::filesystem::exists(append_directory));
  std::filesystem::remove_all(parent);
}

TEST(AppendOnlyLogTest, OpenCreatesInitialFileAndManifestIdempotently) {
  const std::filesystem::path parent =
      MakeTempDirectory("/tmp/redis-aof-openXXXXXX");
  const redis::AppendOnlyLogConfig config = EnabledConfig(parent);
  redis::AppendOnlyLog log(config);

  const redis::Status first_open = log.Open();
  ASSERT_TRUE(first_open.has_value()) << first_open.error().Message();
  ASSERT_TRUE(log.Open().has_value());

  const std::filesystem::path active_file =
      config.directory / "configured.aof.1.incr.aof";
  const std::filesystem::path manifest =
      config.directory / "configured.aof.manifest";
  EXPECT_TRUE(std::filesystem::is_regular_file(active_file));
  EXPECT_EQ(std::filesystem::file_size(active_file), 0U);
  EXPECT_EQ(ReadFile(manifest),
            "file configured.aof.1.incr.aof seq 1 type i\n");
  std::filesystem::remove_all(parent);
}

TEST(AppendOnlyLogTest, AppendUsesActiveFileNamedByExistingManifest) {
  const std::filesystem::path parent =
      MakeTempDirectory("/tmp/redis-aof-manifestXXXXXX");
  const redis::AppendOnlyLogConfig config = EnabledConfig(parent);
  ASSERT_TRUE(std::filesystem::create_directories(config.directory));
  std::ofstream(config.directory / "configured.aof.manifest")
      << "file random-name.1.incr.aof seq 1 type i\n";
  const std::filesystem::path active_file =
      config.directory / "random-name.1.incr.aof";
  std::ofstream(active_file, std::ios::binary);

  redis::AppendOnlyLog log(config);
  ASSERT_TRUE(log.Open().has_value());
  ASSERT_TRUE(log.Append({"SET", "foo", "100"}).has_value());
  ASSERT_TRUE(log.Append({"SET", "bar", "200"}).has_value());

  EXPECT_EQ(ReadFile(active_file),
            redis::RespWriter::WriteCommand({"SET", "foo", "100"}) +
                redis::RespWriter::WriteCommand({"SET", "bar", "200"}));
  EXPECT_FALSE(std::filesystem::exists(
      config.directory / "configured.aof.1.incr.aof"));
  std::filesystem::remove_all(parent);
}

TEST(AppendOnlyLogTest, AppendsTransactionsAsOneFramedBatch) {
  const std::filesystem::path parent =
      MakeTempDirectory("/tmp/redis-aof-transactionXXXXXX");
  const redis::AppendOnlyLogConfig config = EnabledConfig(parent);
  redis::AppendOnlyLog log(config);
  ASSERT_TRUE(log.Open().has_value());

  ASSERT_TRUE(log.AppendTransaction(
                     {{"SET", "foo", "1"}, {"INCR", "foo"}})
                  .has_value());

  const std::filesystem::path active_file =
      config.directory / "configured.aof.1.incr.aof";
  EXPECT_EQ(ReadFile(active_file),
            redis::RespWriter::WriteCommand({"MULTI"}) +
                redis::RespWriter::WriteCommand({"SET", "foo", "1"}) +
                redis::RespWriter::WriteCommand({"INCR", "foo"}) +
                redis::RespWriter::WriteCommand({"EXEC"}));
  std::filesystem::remove_all(parent);
}

TEST(AppendOnlyLogTest, CommandExecutionLogsOnlyWriteCommands) {
  const std::filesystem::path parent =
      MakeTempDirectory("/tmp/redis-aof-executionXXXXXX");
  const redis::AppendOnlyLogConfig config = EnabledConfig(parent);
  redis::AppendOnlyLog log(config);
  ASSERT_TRUE(log.Open().has_value());
  redis::Database database;
  redis::CommandExecutor executor(database, false, nullptr, nullptr, &log);

  ASSERT_TRUE(executor.Execute({"SET", "foo", "1"}).has_value());
  ASSERT_TRUE(executor.Execute({"GET", "foo"}).has_value());
  ASSERT_TRUE(executor.Execute({"PING"}).has_value());
  ASSERT_TRUE(executor.Execute({"ECHO", "hello"}).has_value());
  ASSERT_TRUE(executor.Execute({"PUBLISH", "events", "hello"}).has_value());
  ASSERT_TRUE(executor.Execute({"ZADD", "scores", "2", "member"})
                  .has_value());
  ASSERT_TRUE(executor.Execute({"LPOP", "missing"}).has_value());
  ASSERT_TRUE(executor.Execute({"ZREM", "scores", "missing"}).has_value());

  const std::filesystem::path active_file =
      config.directory / "configured.aof.1.incr.aof";
  EXPECT_EQ(ReadFile(active_file),
            redis::RespWriter::WriteCommand({"SET", "foo", "1"}) +
                redis::RespWriter::WriteCommand(
                    {"ZADD", "scores", "2", "member"}));
  std::filesystem::remove_all(parent);
}

TEST(AppendOnlyLogTest, ExecPersistsTheTransactionInsteadOfIndividualWrites) {
  const std::filesystem::path parent =
      MakeTempDirectory("/tmp/redis-aof-execXXXXXX");
  const redis::AppendOnlyLogConfig config = EnabledConfig(parent);
  redis::AppendOnlyLog log(config);
  ASSERT_TRUE(log.Open().has_value());
  redis::Database database;
  redis::CommandExecutor executor(database, false, nullptr, nullptr, &log);
  redis::Transaction transaction(executor);

  ASSERT_TRUE(transaction.Process({"MULTI"})->has_value());
  ASSERT_TRUE(transaction.Process({"SET", "foo", "1"})->has_value());
  ASSERT_TRUE(
      transaction.Process({"PUBLISH", "events", "hello"})->has_value());
  ASSERT_TRUE(transaction.Process({"INCR", "foo"})->has_value());
  const auto exec = transaction.Process({"EXEC"});
  ASSERT_TRUE(exec.has_value());
  ASSERT_TRUE(exec->has_value());

  const std::filesystem::path active_file =
      config.directory / "configured.aof.1.incr.aof";
  EXPECT_EQ(ReadFile(active_file),
            redis::RespWriter::WriteCommand({"MULTI"}) +
                redis::RespWriter::WriteCommand({"SET", "foo", "1"}) +
                redis::RespWriter::WriteCommand({"INCR", "foo"}) +
                redis::RespWriter::WriteCommand({"EXEC"}));
  std::filesystem::remove_all(parent);
}

TEST(AppendOnlyLogTest, ReplayAppliesEveryCommandInManifestOrder) {
  const std::filesystem::path parent =
      MakeTempDirectory("/tmp/redis-aof-replayXXXXXX");
  const redis::AppendOnlyLogConfig config = EnabledConfig(parent);
  ASSERT_TRUE(std::filesystem::create_directories(config.directory));
  std::ofstream(config.directory / "configured.aof.manifest")
      << "file restored.1.incr.aof seq 1 type i\n";
  const std::filesystem::path active_file =
      config.directory / "restored.1.incr.aof";
  const std::string contents =
      redis::RespWriter::WriteCommand({"SET", "orange", "240"}) +
      redis::RespWriter::WriteCommand({"SET", "pear", "519"}) +
      redis::RespWriter::WriteCommand({"SET", "orange", "777"});
  std::ofstream(active_file, std::ios::binary) << contents;

  redis::AppendOnlyLog log(config);
  redis::Database database;
  redis::CommandExecutor executor(database);
  const redis::Status replay = log.Replay(
      [&](const std::vector<std::string>& command) -> redis::Status {
        redis::CommandResult result = executor.Execute(
            command, redis::CommandOrigin::kAppendOnlyReplay);
        if (!result) {
          return tl::make_unexpected(
              redis::MakeRespError(redis::RespErrorCode::kInvalidFrame));
        }
        return {};
      });

  ASSERT_TRUE(replay.has_value());
  const redis::CommandResult orange = executor.Execute({"GET", "orange"});
  const redis::CommandResult pear = executor.Execute({"GET", "pear"});
  ASSERT_TRUE(orange.has_value());
  ASSERT_TRUE(pear.has_value());
  EXPECT_EQ((*orange).Get<redis::RespBulkString>().value, "777");
  EXPECT_EQ((*pear).Get<redis::RespBulkString>().value, "519");
  EXPECT_EQ(ReadFile(active_file), contents);
  std::filesystem::remove_all(parent);
}

TEST(AppendOnlyLogTest, ReplayRejectsTruncatedCommand) {
  const std::filesystem::path parent =
      MakeTempDirectory("/tmp/redis-aof-truncatedXXXXXX");
  const redis::AppendOnlyLogConfig config = EnabledConfig(parent);
  ASSERT_TRUE(std::filesystem::create_directories(config.directory));
  std::ofstream(config.directory / "configured.aof.manifest")
      << "file truncated.1.incr.aof seq 1 type i\n";
  const std::filesystem::path active_file =
      config.directory / "truncated.1.incr.aof";
  std::ofstream(active_file, std::ios::binary) << "*3\r\n$3\r\nSET\r\n";

  redis::AppendOnlyLog log(config);
  const redis::Status replay = log.Replay(
      [](const std::vector<std::string>&) -> redis::Status { return {}; });

  const redis::FileSystemError* error = FileError(replay);
  ASSERT_NE(error, nullptr);
  EXPECT_EQ(error->code, redis::FileSystemErrorCode::kInvalidFileFormat);
  EXPECT_EQ(error->path, active_file.string());
  std::filesystem::remove_all(parent);
}

TEST(AppendOnlyLogTest, ReplayRejectsNonRespCommands) {
  const std::filesystem::path parent =
      MakeTempDirectory("/tmp/redis-aof-inlineXXXXXX");
  const redis::AppendOnlyLogConfig config = EnabledConfig(parent);
  ASSERT_TRUE(std::filesystem::create_directories(config.directory));
  std::ofstream(config.directory / "configured.aof.manifest")
      << "file active.1.incr.aof seq 1 type i\n";
  std::ofstream(config.directory / "active.1.incr.aof", std::ios::binary)
      << "PING\r\n";

  redis::AppendOnlyLog log(config);
  ASSERT_TRUE(log.Open().has_value());

  const redis::Status replay_status = log.Replay(
      [](const std::vector<std::string>&) -> redis::Status { return {}; });
  EXPECT_FALSE(replay_status.has_value());
  std::filesystem::remove_all(parent);
}

TEST(AppendOnlyLogTest, ReplayDoesNotApplyIncompleteTransactions) {
  const std::filesystem::path parent =
      MakeTempDirectory("/tmp/redis-aof-incomplete-transactionXXXXXX");
  const redis::AppendOnlyLogConfig config = EnabledConfig(parent);
  ASSERT_TRUE(std::filesystem::create_directories(config.directory));
  std::ofstream(config.directory / "configured.aof.manifest")
      << "file active.1.incr.aof seq 1 type i\n";
  std::ofstream(config.directory / "active.1.incr.aof", std::ios::binary)
      << redis::RespWriter::WriteCommand({"MULTI"})
      << redis::RespWriter::WriteCommand({"SET", "foo", "1"});

  redis::AppendOnlyLog log(config);
  int executed_commands = 0;
  const redis::Status replay_status = log.Replay(
      [&](const std::vector<std::string>&) -> redis::Status {
        ++executed_commands;
        return {};
      });

  EXPECT_FALSE(replay_status.has_value());
  EXPECT_EQ(executed_commands, 0);
  std::filesystem::remove_all(parent);
}

TEST(AppendOnlyLogTest, InvalidManifestReturnsActionableError) {
  const std::filesystem::path parent =
      MakeTempDirectory("/tmp/redis-aof-invalid-manifestXXXXXX");
  const redis::AppendOnlyLogConfig config = EnabledConfig(parent);
  ASSERT_TRUE(std::filesystem::create_directories(config.directory));
  const std::filesystem::path manifest =
      config.directory / "configured.aof.manifest";
  std::ofstream(manifest) << "file ../escape.aof seq 1 type i\n";

  redis::AppendOnlyLog log(config);
  const redis::Status status = log.Open();

  const redis::FileSystemError* error = FileError(status);
  ASSERT_NE(error, nullptr);
  EXPECT_EQ(error->code, redis::FileSystemErrorCode::kInvalidFileFormat);
  EXPECT_EQ(error->path, manifest.string());
  std::filesystem::remove_all(parent);
}

TEST(AppendOnlyLogTest, RejectsManifestSymlink) {
  const std::filesystem::path parent =
      MakeTempDirectory("/tmp/redis-aof-manifest-symlinkXXXXXX");
  const redis::AppendOnlyLogConfig config = EnabledConfig(parent);
  ASSERT_TRUE(std::filesystem::create_directories(config.directory));
  const std::filesystem::path manifest_target = parent / "manifest-target";
  std::ofstream(manifest_target)
      << "file active.1.incr.aof seq 1 type i\n";
  const std::filesystem::path manifest =
      config.directory / "configured.aof.manifest";
  std::error_code link_error;
  std::filesystem::create_symlink(manifest_target, manifest, link_error);
  ASSERT_FALSE(link_error);

  redis::AppendOnlyLog log(config);
  const redis::Status status = log.Open();

  const redis::FileSystemError* error = FileError(status);
  ASSERT_NE(error, nullptr);
  EXPECT_EQ(error->path, manifest.string());
  std::filesystem::remove_all(parent);
}

TEST(AppendOnlyLogTest, RejectsParentDirectoryEntriesInManifest) {
  const std::filesystem::path parent =
      MakeTempDirectory("/tmp/redis-aof-parent-entryXXXXXX");
  const redis::AppendOnlyLogConfig config = EnabledConfig(parent);
  ASSERT_TRUE(std::filesystem::create_directories(config.directory));
  const std::filesystem::path manifest =
      config.directory / "configured.aof.manifest";
  std::ofstream(manifest) << "file .. seq 1 type i\n";

  redis::AppendOnlyLog log(config);
  const redis::Status status = log.Open();

  ASSERT_FALSE(status.has_value());
  const auto* error =
      std::get_if<redis::FileSystemError>(&status.error().Kind());
  ASSERT_NE(error, nullptr);
  EXPECT_EQ(error->code, redis::FileSystemErrorCode::kInvalidFileFormat);
  std::filesystem::remove_all(parent);
}

TEST(AppendOnlyLogTest, RejectsActiveFileSymlinkWithoutTouchingItsTarget) {
  const std::filesystem::path parent =
      MakeTempDirectory("/tmp/redis-aof-active-symlinkXXXXXX");
  const redis::AppendOnlyLogConfig config = EnabledConfig(parent);
  ASSERT_TRUE(std::filesystem::create_directories(config.directory));
  std::ofstream(config.directory / "configured.aof.manifest")
      << "file active.1.incr.aof seq 1 type i\n";
  const std::filesystem::path target = parent / "outside.aof";
  std::ofstream(target, std::ios::binary) << "unchanged";
  const std::filesystem::path active_file =
      config.directory / "active.1.incr.aof";
  std::error_code link_error;
  std::filesystem::create_symlink(target, active_file, link_error);
  ASSERT_FALSE(link_error);

  redis::AppendOnlyLog log(config);
  const redis::Status status = log.Open();

  const redis::FileSystemError* error = FileError(status);
  ASSERT_NE(error, nullptr);
  EXPECT_EQ(error->path, active_file.string());
  EXPECT_EQ(ReadFile(target), "unchanged");
  std::filesystem::remove_all(parent);
}

TEST(AppendOnlyLogTest, RejectsUnsafeConfiguredFilename) {
  const std::filesystem::path parent =
      MakeTempDirectory("/tmp/redis-aof-invalid-pathXXXXXX");
  redis::AppendOnlyLogConfig config = EnabledConfig(parent);
  config.base_filename = "../escape.aof";

  redis::AppendOnlyLog log(config);
  const redis::Status status = log.Open();

  const redis::FileSystemError* error = FileError(status);
  ASSERT_NE(error, nullptr);
  EXPECT_EQ(error->code, redis::FileSystemErrorCode::kInvalidPath);
  std::filesystem::remove_all(parent);
}

TEST(AppendOnlyLogTest, DirectoryCreationFailureReturnsActionableError) {
  const std::filesystem::path parent =
      MakeTempDirectory("/tmp/redis-aof-create-failureXXXXXX");
  const std::filesystem::path blocker = parent / "blocker";
  std::ofstream(blocker) << "not a directory";
  redis::AppendOnlyLogConfig config = EnabledConfig(parent);
  config.directory = blocker / "appendonlydir";
  redis::AppendOnlyLog log(config);

  const redis::Status status = log.Open();

  const redis::FileSystemError* error = FileError(status);
  ASSERT_NE(error, nullptr);
  EXPECT_EQ(error->code,
            redis::FileSystemErrorCode::kCreateDirectoryFailed);
  EXPECT_EQ(error->path, config.directory.string());
  std::filesystem::remove_all(parent);
}

}  // namespace
