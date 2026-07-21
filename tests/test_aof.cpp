#include "test_support.hpp"

#include "redis-cpp/aof.hpp"

namespace {

TEST(AofTest, CreatesConfiguredDirectoryAndFileWhenAppendOnlyIsEnabled) {
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

  const redis::Status result = redis::PrepareAofStorage(config);

  EXPECT_TRUE(result.has_value());
  EXPECT_TRUE(std::filesystem::is_directory(expected_directory));
  EXPECT_TRUE(std::filesystem::is_regular_file(expected_file));
  EXPECT_EQ(std::filesystem::file_size(expected_file), 0U);
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

}  // namespace
