#include "test_support.hpp"

namespace {

using redis::CommandExecutor;
using redis::Database;
using redis::RespArray;
using redis::ServerConfig;
using redis::RespWriter;

void AppendLittleEndian64(std::vector<unsigned char>& bytes, uint64_t value) {
  for (int shift = 0; shift < 64; shift += 8) {
    bytes.push_back(static_cast<unsigned char>((value >> shift) & 0xFF));
  }
}

TEST(RdbLoaderTest, ImportsSingleStringKey) {
  Database database;
  ServerConfig config;

  char directory_template[] = "/tmp/redis-rdb-testXXXXXX";
  char* directory = mkdtemp(directory_template);
  ASSERT_TRUE((directory != nullptr)) << "mkdtemp should succeed";

  config.dir = directory;
  config.dbfilename = "dump.rdb";
  const std::filesystem::path file_path =
      std::filesystem::path(config.dir) / config.dbfilename;

  const std::vector<unsigned char> rdb_bytes = {
      'R',  'E',  'D',  'I',  'S',  '0',  '0',  '1',  '1',
      0xFE, 0x00, 0xFB, 0x01, 0x00, 0x00, 0x03, 'f',  'o',
      'o',  0x03, 'b',  'a',  'r',  0xFF, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00,
  };
  {
    std::ofstream output(file_path, std::ios::binary);
    output.write(reinterpret_cast<const char*>(rdb_bytes.data()),
                 static_cast<std::streamsize>(rdb_bytes.size()));
  }

  ASSERT_TRUE(LoadDatabaseFromRdb(config, database).has_value());
  CommandExecutor executor(database, false);

  redis::CommandResult result = executor.Execute({"KEYS", "*"});
  ASSERT_TRUE((result.has_value())) << "KEYS * should succeed after loading RDB";
  ASSERT_TRUE((result->Is<RespArray>())) << "KEYS * should return a RESP array after loading RDB";
  ASSERT_TRUE((RespBulkStrings(result->Get<RespArray>()) ==
             std::vector<std::string>({"foo"}))) << "RDB loader should import the single key from the RDB file";

  std::filesystem::remove(file_path);
  std::filesystem::remove(config.dir);
}

TEST(RdbLoaderTest, MakesLoadedValueAvailableToGet) {
  Database database;
  ServerConfig config;

  char directory_template[] = "/tmp/redis-rdb-get-testXXXXXX";
  char* directory = mkdtemp(directory_template);
  ASSERT_TRUE((directory != nullptr)) << "mkdtemp should succeed";

  config.dir = directory;
  config.dbfilename = "dump.rdb";
  const std::filesystem::path file_path =
      std::filesystem::path(config.dir) / config.dbfilename;

  const std::vector<unsigned char> rdb_bytes = {
      'R',  'E',  'D',  'I',  'S',  '0',  '0',  '1',  '1',
      0xFE, 0x00, 0xFB, 0x01, 0x00, 0x00, 0x03, 'f',  'o',
      'o',  0x03, 'b',  'a',  'r',  0xFF, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00,
  };
  {
    std::ofstream output(file_path, std::ios::binary);
    output.write(reinterpret_cast<const char*>(rdb_bytes.data()),
                 static_cast<std::streamsize>(rdb_bytes.size()));
  }

  ASSERT_TRUE(LoadDatabaseFromRdb(config, database).has_value());
  CommandExecutor executor(database, false);

  redis::CommandResult result = executor.Execute({"GET", "foo"});
  ASSERT_TRUE((result.has_value())) << "GET foo should succeed after loading RDB";
  ASSERT_TRUE((result->Is<redis::RespBulkString>())) << "GET foo should return a RESP bulk string after loading RDB";
  ASSERT_TRUE((result->Get<redis::RespBulkString>().value == "bar")) << "GET foo should return the loaded value from the RDB file";
  ASSERT_TRUE((RespWriter::Write(*result) == "$3\r\nbar\r\n")) << "GET foo should encode the loaded value as a RESP bulk string";

  std::filesystem::remove(file_path);
  std::filesystem::remove(config.dir);
}

TEST(RdbLoaderTest, ImportsMultipleStringValues) {
  Database database;
  ServerConfig config;

  char directory_template[] = "/tmp/redis-rdb-multi-testXXXXXX";
  char* directory = mkdtemp(directory_template);
  ASSERT_TRUE((directory != nullptr)) << "mkdtemp should succeed";

  config.dir = directory;
  config.dbfilename = "dump.rdb";
  const std::filesystem::path file_path =
      std::filesystem::path(config.dir) / config.dbfilename;

  const std::vector<unsigned char> rdb_bytes = {
      'R',  'E',  'D',  'I',  'S',  '0',  '0',  '1',  '1',
      0xFE, 0x00, 0xFB, 0x02, 0x00,
      0x00, 0x03, 'f',  'o',  'o',  0x03, 'o',  'n',  'e',
      0x00, 0x03, 'b',  'a',  'r',  0x03, 't',  'w',  'o',
      0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
  };
  {
    std::ofstream output(file_path, std::ios::binary);
    output.write(reinterpret_cast<const char*>(rdb_bytes.data()),
                 static_cast<std::streamsize>(rdb_bytes.size()));
  }

  ASSERT_TRUE(LoadDatabaseFromRdb(config, database).has_value());
  CommandExecutor executor(database, false);

  redis::CommandResult foo_result = executor.Execute({"GET", "foo"});
  ASSERT_TRUE((foo_result.has_value())) << "GET foo should succeed after loading RDB";
  ASSERT_TRUE((foo_result->Is<redis::RespBulkString>())) << "GET foo should return a RESP bulk string";
  ASSERT_TRUE((foo_result->Get<redis::RespBulkString>().value == "one")) << "GET foo should return the first loaded value";

  redis::CommandResult bar_result = executor.Execute({"GET", "bar"});
  ASSERT_TRUE((bar_result.has_value())) << "GET bar should succeed after loading RDB";
  ASSERT_TRUE((bar_result->Is<redis::RespBulkString>())) << "GET bar should return a RESP bulk string";
  ASSERT_TRUE((bar_result->Get<redis::RespBulkString>().value == "two")) << "GET bar should return the second loaded value";

  std::filesystem::remove(file_path);
  std::filesystem::remove(config.dir);
}

TEST(RdbLoaderTest, RespectsExpiredAndLiveKeys) {
  Database database;
  ServerConfig config;

  char directory_template[] = "/tmp/redis-rdb-expiry-testXXXXXX";
  char* directory = mkdtemp(directory_template);
  ASSERT_TRUE((directory != nullptr)) << "mkdtemp should succeed";

  config.dir = directory;
  config.dbfilename = "dump.rdb";
  const std::filesystem::path file_path =
      std::filesystem::path(config.dir) / config.dbfilename;

  const auto now = std::chrono::time_point_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now());
  const uint64_t now_ms =
      static_cast<uint64_t>(now.time_since_epoch().count());
  const uint64_t expired_ms = now_ms - 60000;
  const uint64_t live_ms = now_ms + 60000;

  std::vector<unsigned char> rdb_bytes = {
      'R',  'E',  'D',  'I',  'S',  '0',  '0',  '1',  '1',
      0xFE, 0x00, 0xFB, 0x02, 0x01,
      0xFC,
  };
  AppendLittleEndian64(rdb_bytes, expired_ms);
  rdb_bytes.insert(rdb_bytes.end(),
                   {0x00, 0x03, 'f', 'o', 'o', 0x03, 'o', 'l', 'd'});
  rdb_bytes.push_back(0xFC);
  AppendLittleEndian64(rdb_bytes, live_ms);
  rdb_bytes.insert(rdb_bytes.end(),
                   {0x00, 0x03, 'b', 'a', 'r', 0x03, 'n', 'e', 'w',
                    0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00});

  {
    std::ofstream output(file_path, std::ios::binary);
    output.write(reinterpret_cast<const char*>(rdb_bytes.data()),
                 static_cast<std::streamsize>(rdb_bytes.size()));
  }

  ASSERT_TRUE(LoadDatabaseFromRdb(config, database).has_value());
  CommandExecutor executor(database, false);

  redis::CommandResult expired_result = executor.Execute({"GET", "foo"});
  ASSERT_TRUE((expired_result.has_value())) << "GET foo should succeed after loading RDB";
  ASSERT_TRUE((expired_result->Is<redis::RespNullBulk>())) << "GET foo should return a null bulk string for expired data";
  ASSERT_TRUE((RespWriter::Write(*expired_result) == "$-1\r\n")) << "Expired keys loaded from RDB should encode as null bulk strings";

  redis::CommandResult live_result = executor.Execute({"GET", "bar"});
  ASSERT_TRUE((live_result.has_value())) << "GET bar should succeed after loading RDB";
  ASSERT_TRUE((live_result->Is<redis::RespBulkString>())) << "GET bar should return a RESP bulk string for live data";
  ASSERT_TRUE((live_result->Get<redis::RespBulkString>().value == "new")) << "GET bar should return the non-expired RDB value";

  std::filesystem::remove(file_path);
  std::filesystem::remove(config.dir);
}

TEST(RdbLoaderTest, RejectsMalformedInputWithoutApplyingPartialState) {
  Database database;
  ServerConfig config;

  char directory_template[] = "/tmp/redis-rdb-invalid-testXXXXXX";
  char* directory = mkdtemp(directory_template);
  ASSERT_NE(directory, nullptr);
  config.dir = directory;
  config.dbfilename = "dump.rdb";
  const std::filesystem::path file_path =
      std::filesystem::path(config.dir) / config.dbfilename;

  const std::vector<unsigned char> bytes = {
      'R', 'E', 'D', 'I', 'S', '0', '0', '1', '1',
      0x00, 0x03, 'f', 'o', 'o', 0x03, 'b', 'a', 'r',
      0x00, 0x03, 'b', 'a', 'z', 0x08, 't', 'r',
  };
  {
    std::ofstream output(file_path, std::ios::binary);
    output.write(reinterpret_cast<const char*>(bytes.data()),
                 static_cast<std::streamsize>(bytes.size()));
  }

  const redis::Status result = LoadDatabaseFromRdb(config, database);

  ASSERT_FALSE(result.has_value());
  CommandExecutor executor(database);
  const redis::CommandResult get = executor.Execute({"GET", "foo"});
  ASSERT_TRUE(get.has_value());
  EXPECT_TRUE(get->Is<redis::RespNullBulk>());

  std::filesystem::remove(file_path);
  std::filesystem::remove(config.dir);
}

}  // namespace
