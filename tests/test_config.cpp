#include "test_support.hpp"

#include "redis-cpp/config_parser.hpp"

namespace {

using redis::CommandExecutor;
using redis::Database;
using redis::RespArray;
using redis::ServerConfig;
using redis::RespWriter;

TEST(ConfigTest, StartupDefaultsIncludeCurrentDirectoryAndAofOptions) {
  char program_name[] = "redis";
  char* argv[] = {program_name};

  const redis::Result<ServerConfig> result =
      redis::ConfigParser{}.Parse(1, argv);

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->dir, std::filesystem::current_path().string());
  EXPECT_EQ(result->appendonly, "no");
  EXPECT_EQ(result->appenddirname, "appendonlydir");
  EXPECT_EQ(result->appendfilename, "appendonly.aof");
  EXPECT_EQ(result->appendfsync, "everysec");
}

TEST(ConfigTest, ConfigGetReturnsAofDefaults) {
  Database database;
  ServerConfig config;
  CommandExecutor executor(database, false, nullptr, &config);

  const std::vector<std::pair<std::string, std::string>> defaults = {
      {"appendonly", "no"},
      {"appenddirname", "appendonlydir"},
      {"appendfilename", "appendonly.aof"},
      {"appendfsync", "everysec"},
  };

  for (const auto& [option, value] : defaults) {
    redis::CommandResult result =
        executor.Execute({"CONFIG", "GET", option});
    ASSERT_TRUE(result.has_value()) << option;
    ASSERT_TRUE(std::holds_alternative<RespArray>(*result)) << option;
    EXPECT_EQ(std::get<RespArray>(*result).values,
              (std::vector<std::string>{option, value}))
        << option;
    EXPECT_EQ(RespWriter::Write(*result),
              "*2\r\n$" + std::to_string(option.size()) + "\r\n" + option +
                  "\r\n$" + std::to_string(value.size()) + "\r\n" + value +
                  "\r\n")
        << option;
  }
}

TEST(ConfigTest, ConfigGetDirReturnsConfiguredDirectory) {
  Database database;
  ServerConfig config;
  config.dir = "/tmp/redis-files";
  CommandExecutor executor(database, false, nullptr, &config);

  redis::CommandResult result = executor.Execute({"CONFIG", "GET", "dir"});
  ASSERT_TRUE((result.has_value())) << "CONFIG GET dir should succeed";
  ASSERT_TRUE((std::holds_alternative<RespArray>(*result))) << "CONFIG GET dir should return a RESP array";

  const auto& response = std::get<RespArray>(*result);
  ASSERT_TRUE((response.values == std::vector<std::string>({"dir", "/tmp/redis-files"}))) << "CONFIG GET dir should return the configured dir";
  ASSERT_TRUE((RespWriter::Write(*result) ==
             "*2\r\n$3\r\ndir\r\n$16\r\n/tmp/redis-files\r\n")) << "CONFIG GET dir should encode as the expected RESP array";
}

TEST(ConfigTest, ConfigGetDbfilenameReturnsConfiguredFilename) {
  Database database;
  ServerConfig config;
  config.dbfilename = "dump.rdb";
  CommandExecutor executor(database, false, nullptr, &config);

  redis::CommandResult result =
      executor.Execute({"CONFIG", "GET", "dbfilename"});
  ASSERT_TRUE((result.has_value())) << "CONFIG GET dbfilename should succeed";
  ASSERT_TRUE((std::holds_alternative<RespArray>(*result))) << "CONFIG GET dbfilename should return a RESP array";

  const auto& response = std::get<RespArray>(*result);
  ASSERT_TRUE((response.values ==
             std::vector<std::string>({"dbfilename", "dump.rdb"}))) << "CONFIG GET dbfilename should return the configured filename";
  ASSERT_TRUE((RespWriter::Write(*result) ==
             "*2\r\n$10\r\ndbfilename\r\n$8\r\ndump.rdb\r\n")) << "CONFIG GET dbfilename should encode as the expected RESP array";
}

TEST(ConfigTest, KeysReturnsStoredKeys) {
  Database database;
  database.SetString("foo", "123");
  CommandExecutor executor(database, false);

  redis::CommandResult result = executor.Execute({"KEYS", "*"});
  ASSERT_TRUE((result.has_value())) << "KEYS * should succeed";
  ASSERT_TRUE((std::holds_alternative<RespArray>(*result))) << "KEYS * should return a RESP array";

  const auto& response = std::get<RespArray>(*result);
  ASSERT_TRUE((response.values == std::vector<std::string>({"foo"}))) << "KEYS * should return the stored key";
  ASSERT_TRUE((RespWriter::Write(*result) == "*1\r\n$3\r\nfoo\r\n")) << "KEYS * should encode the key as a RESP array";
}

}  // namespace
