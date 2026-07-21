#include "test_support.hpp"

#include <iterator>

#include "redis-cpp/aof.hpp"

namespace {

TEST(AofTest, CreatesConfiguredDirectoryAofFileAndManifestWhenEnabled) {
  char parent_template[] = "/tmp/redis-aof-testXXXXXX";
  char* parent = mkdtemp(parent_template);
  ASSERT_NE(parent, nullptr);

  redis::ServerConfig config;
  config.dir = (std::filesystem::path(parent) / "data").string();
  config.appendonly = "yes";
  config.appenddirname = "custom-append-dir";
  config.appendfilename = "custom.aof";
  const std::filesystem::path expected_directory =
      std::filesystem::path(config.dir) / config.appenddirname;
  const std::filesystem::path expected_file =
      expected_directory / "custom.aof.1.incr.aof";
  const std::filesystem::path expected_manifest =
      expected_directory / "custom.aof.manifest";

  const redis::Status result = redis::PrepareAofStorage(config);

  EXPECT_TRUE(result.has_value());
  EXPECT_TRUE(std::filesystem::is_directory(expected_directory));
  EXPECT_TRUE(std::filesystem::is_regular_file(expected_file));
  EXPECT_EQ(std::filesystem::file_size(expected_file), 0U);
  EXPECT_TRUE(std::filesystem::is_regular_file(expected_manifest));
  std::ifstream manifest(expected_manifest);
  EXPECT_EQ(std::string(std::istreambuf_iterator<char>(manifest),
                        std::istreambuf_iterator<char>()),
            "file custom.aof.1.incr.aof seq 1 type i\n");
  std::filesystem::remove_all(parent);
}

TEST(AofTest, DoesNotCreateDirectoryWhenAppendOnlyIsDisabled) {
  char parent_template[] = "/tmp/redis-aof-disabled-testXXXXXX";
  char* parent = mkdtemp(parent_template);
  ASSERT_NE(parent, nullptr);

  redis::ServerConfig config;
  config.dir = parent;
  config.appendonly = "no";
  config.appenddirname = "appendonlydir";
  const std::filesystem::path append_directory =
      std::filesystem::path(config.dir) / config.appenddirname;

  const redis::Status result = redis::PrepareAofStorage(config);

  EXPECT_TRUE(result.has_value());
  EXPECT_FALSE(std::filesystem::exists(append_directory));
  std::filesystem::remove_all(parent);
}

TEST(AofTest, ExistingAppendOnlyFileIsNotTruncated) {
  char parent_template[] = "/tmp/redis-aof-existing-testXXXXXX";
  char* parent = mkdtemp(parent_template);
  ASSERT_NE(parent, nullptr);

  redis::ServerConfig config;
  config.dir = parent;
  config.appendonly = "yes";
  config.appenddirname = "appendonlydir";

  ASSERT_TRUE(redis::PrepareAofStorage(config).has_value());
  const std::filesystem::path append_file =
      std::filesystem::path(config.dir) / config.appenddirname /
      "appendonly.aof.1.incr.aof";
  std::ofstream(append_file) << "existing contents";

  EXPECT_TRUE(redis::PrepareAofStorage(config).has_value());
  EXPECT_EQ(std::filesystem::file_size(append_file), 17U);
  std::filesystem::remove_all(parent);
}

TEST(AofTest, DirectoryCreationFailureReturnsActionableError) {
  char parent_template[] = "/tmp/redis-aof-error-testXXXXXX";
  char* parent = mkdtemp(parent_template);
  ASSERT_NE(parent, nullptr);

  const std::filesystem::path blocking_file =
      std::filesystem::path(parent) / "not-a-directory";
  std::ofstream(blocking_file) << "contents";

  redis::ServerConfig config;
  config.dir = blocking_file.string();
  config.appendonly = "yes";
  config.appenddirname = "appendonlydir";

  const redis::Status result = redis::PrepareAofStorage(config);

  ASSERT_FALSE(result.has_value());
  const auto* error =
      std::get_if<redis::FileSystemError>(&result.error().Kind());
  ASSERT_NE(error, nullptr);
  EXPECT_EQ(error->code,
            redis::FileSystemErrorCode::kCreateDirectoryFailed);
  EXPECT_EQ(error->path, (blocking_file / config.appenddirname).string());
  std::filesystem::remove_all(parent);
}

TEST(AofTest, FileCreationFailureReturnsActionableError) {
  char parent_template[] = "/tmp/redis-aof-file-error-testXXXXXX";
  char* parent = mkdtemp(parent_template);
  ASSERT_NE(parent, nullptr);

  redis::ServerConfig config;
  config.dir = parent;
  config.appendonly = "yes";
  config.appenddirname = "appendonlydir";
  config.appendfilename = "missing/appendonly.aof";
  const std::filesystem::path expected_file =
      std::filesystem::path(config.dir) / config.appenddirname /
      "missing/appendonly.aof.1.incr.aof";

  const redis::Status result = redis::PrepareAofStorage(config);

  ASSERT_FALSE(result.has_value());
  const auto* error =
      std::get_if<redis::FileSystemError>(&result.error().Kind());
  ASSERT_NE(error, nullptr);
  EXPECT_EQ(error->code, redis::FileSystemErrorCode::kCreateFileFailed);
  EXPECT_EQ(error->path, expected_file.string());
  std::filesystem::remove_all(parent);
}

TEST(AofTest, ManifestCreationFailureReturnsActionableError) {
  char parent_template[] = "/tmp/redis-aof-manifest-error-testXXXXXX";
  char* parent = mkdtemp(parent_template);
  ASSERT_NE(parent, nullptr);

  redis::ServerConfig config;
  config.dir = parent;
  config.appendonly = "yes";
  config.appenddirname = "appendonlydir";
  config.appendfilename = "appendonly.aof";
  const std::filesystem::path manifest_path =
      std::filesystem::path(config.dir) / config.appenddirname /
      "appendonly.aof.manifest";
  ASSERT_TRUE(std::filesystem::create_directories(manifest_path));

  const redis::Status result = redis::PrepareAofStorage(config);

  ASSERT_FALSE(result.has_value());
  const auto* error =
      std::get_if<redis::FileSystemError>(&result.error().Kind());
  ASSERT_NE(error, nullptr);
  EXPECT_EQ(error->code, redis::FileSystemErrorCode::kCreateFileFailed);
  EXPECT_EQ(error->path, manifest_path.string());
  std::filesystem::remove_all(parent);
}

TEST(AofTest, OnlySetsAppendInOrderWhenReadsAreInterleaved) {
  char parent_template[] = "/tmp/redis-aof-append-testXXXXXX";
  char* parent = mkdtemp(parent_template);
  ASSERT_NE(parent, nullptr);

  redis::ServerConfig config;
  config.dir = parent;
  config.appendonly = "yes";
  config.appenddirname = "appendonlydir";
  config.appendfilename = "configured.aof";
  config.appendfsync = "always";
  const std::filesystem::path append_directory =
      std::filesystem::path(config.dir) / config.appenddirname;
  ASSERT_TRUE(std::filesystem::create_directories(append_directory));

  const std::filesystem::path manifest_path =
      append_directory / "configured.aof.manifest";
  const std::string manifest_contents =
      "file active-random.1.incr.aof seq 1 type i\n";
  std::ofstream(manifest_path) << manifest_contents;
  const std::filesystem::path active_file =
      append_directory / "active-random.1.incr.aof";
  std::ofstream initial_aof(active_file);
  ASSERT_TRUE(initial_aof.is_open());
  initial_aof.close();

  ASSERT_TRUE(redis::PrepareAofStorage(config).has_value());
  redis::AofWriter writer(config);
  redis::Database database;
  redis::CommandExecutor executor(database, false, nullptr, &config, &writer);

  const redis::CommandResult first_result =
      executor.Execute({"SET", "foo", "100"});
  const redis::CommandResult get_result = executor.Execute({"GET", "foo"});
  const redis::CommandResult ping_result = executor.Execute({"PING"});
  const redis::CommandResult echo_result =
      executor.Execute({"ECHO", "hello"});
  const redis::CommandResult config_result =
      executor.Execute({"CONFIG", "GET", "appendonly"});
  const redis::CommandResult second_result =
      executor.Execute({"SET", "bar", "200"});

  ASSERT_TRUE(first_result.has_value());
  ASSERT_TRUE(get_result.has_value());
  ASSERT_TRUE(ping_result.has_value());
  ASSERT_TRUE(echo_result.has_value());
  ASSERT_TRUE(config_result.has_value());
  ASSERT_TRUE(second_result.has_value());
  std::ifstream aof(active_file, std::ios::binary);
  EXPECT_EQ(std::string(std::istreambuf_iterator<char>(aof),
                        std::istreambuf_iterator<char>()),
            "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n100\r\n"
            "*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$3\r\n200\r\n");
  std::ifstream manifest(manifest_path);
  EXPECT_EQ(std::string(std::istreambuf_iterator<char>(manifest),
                        std::istreambuf_iterator<char>()),
            manifest_contents);
  EXPECT_FALSE(std::filesystem::exists(
      append_directory / "configured.aof.1.incr.aof"));
  std::filesystem::remove_all(parent);
}

TEST(AofTest, InvalidManifestMakesSetReturnPersistenceError) {
  char parent_template[] = "/tmp/redis-aof-invalid-manifest-testXXXXXX";
  char* parent = mkdtemp(parent_template);
  ASSERT_NE(parent, nullptr);

  redis::ServerConfig config;
  config.dir = parent;
  config.appendonly = "yes";
  config.appenddirname = "appendonlydir";
  const std::filesystem::path append_directory =
      std::filesystem::path(config.dir) / config.appenddirname;
  ASSERT_TRUE(std::filesystem::create_directories(append_directory));
  std::ofstream(append_directory / "appendonly.aof.manifest")
      << "not a valid manifest\n";

  redis::AofWriter writer(config);
  redis::Database database;
  redis::CommandExecutor executor(database, false, nullptr, &config, &writer);

  const redis::CommandResult result = executor.Execute({"SET", "foo", "100"});

  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code,
            redis::CommandErrorCode::kPersistenceFailed);
  EXPECT_NE(result.error().detail.find("Invalid file format"),
            std::string::npos);
  std::filesystem::remove_all(parent);
}

TEST(AofTest, ReplaysAllSetsFromIncrementalFileNamedByManifest) {
  char parent_template[] = "/tmp/redis-aof-replay-testXXXXXX";
  char* parent = mkdtemp(parent_template);
  ASSERT_NE(parent, nullptr);

  redis::ServerConfig config;
  config.dir = parent;
  config.appendonly = "yes";
  config.appenddirname = "appendonlydir";
  config.appendfilename = "configured.aof";
  const std::filesystem::path append_directory =
      std::filesystem::path(config.dir) / config.appenddirname;
  ASSERT_TRUE(std::filesystem::create_directories(append_directory));
  std::ofstream(append_directory / "configured.aof.manifest")
      << "file random-name.1.incr.aof seq 1 type i\n";
  const std::filesystem::path active_file =
      append_directory / "random-name.1.incr.aof";
  const std::string aof_contents =
      "*3\r\n$3\r\nSET\r\n$6\r\norange\r\n$3\r\n240\r\n"
      "*3\r\n$3\r\nSET\r\n$4\r\npear\r\n$3\r\n519\r\n";
  std::ofstream(active_file, std::ios::binary) << aof_contents;

  redis::Database database;
  redis::CommandExecutor replay_executor(database, false, nullptr, &config);

  const redis::Status replay_result =
      redis::ReplayAof(config, replay_executor);
  const redis::CommandResult orange_result =
      replay_executor.Execute({"GET", "orange"});
  const redis::CommandResult pear_result =
      replay_executor.Execute({"GET", "pear"});

  ASSERT_TRUE(replay_result.has_value());
  ASSERT_TRUE(orange_result.has_value());
  ASSERT_TRUE(pear_result.has_value());
  ASSERT_TRUE(std::holds_alternative<redis::RespBulkString>(*orange_result));
  ASSERT_TRUE(std::holds_alternative<redis::RespBulkString>(*pear_result));
  EXPECT_EQ(std::get<redis::RespBulkString>(*orange_result).value, "240");
  EXPECT_EQ(std::get<redis::RespBulkString>(*pear_result).value, "519");
  std::ifstream aof(active_file, std::ios::binary);
  EXPECT_EQ(std::string(std::istreambuf_iterator<char>(aof),
                        std::istreambuf_iterator<char>()),
            aof_contents);
  std::filesystem::remove_all(parent);
}

TEST(AofTest, RejectsTruncatedCommandDuringReplay) {
  char parent_template[] = "/tmp/redis-aof-truncated-testXXXXXX";
  char* parent = mkdtemp(parent_template);
  ASSERT_NE(parent, nullptr);

  redis::ServerConfig config;
  config.dir = parent;
  config.appendonly = "yes";
  config.appenddirname = "appendonlydir";
  const std::filesystem::path append_directory =
      std::filesystem::path(config.dir) / config.appenddirname;
  ASSERT_TRUE(std::filesystem::create_directories(append_directory));
  std::ofstream(append_directory / "appendonly.aof.manifest")
      << "file truncated.1.incr.aof seq 1 type i\n";
  const std::filesystem::path active_file =
      append_directory / "truncated.1.incr.aof";
  std::ofstream(active_file, std::ios::binary) << "*3\r\n$3\r\nSET\r\n";

  redis::Database database;
  redis::CommandExecutor replay_executor(database);

  const redis::Status result = redis::ReplayAof(config, replay_executor);

  ASSERT_FALSE(result.has_value());
  const auto* error =
      std::get_if<redis::FileSystemError>(&result.error().Kind());
  ASSERT_NE(error, nullptr);
  EXPECT_EQ(error->code, redis::FileSystemErrorCode::kInvalidFileFormat);
  EXPECT_EQ(error->path, active_file.string());
  std::filesystem::remove_all(parent);
}

}  // namespace
