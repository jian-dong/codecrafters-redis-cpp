#include "test_support.hpp"

namespace {

using redis::CommandErrorCode;
using redis::CommandExecutor;
using redis::Database;
using redis::RespSimpleString;
using redis::RespWriter;

TEST(TransactionTest, WatchSingleKeyReturnsOk) {
  Database database;
  CommandExecutor executor(database);

  redis::CommandResult result = executor.Execute({"WATCH", "key"});

  ASSERT_TRUE(result.has_value());
  ASSERT_TRUE(std::holds_alternative<RespSimpleString>(*result));
  EXPECT_EQ(std::get<RespSimpleString>(*result).value, "OK");
  EXPECT_EQ(RespWriter::Write(*result), "+OK\r\n");
}

TEST(TransactionTest, WatchCommandIsCaseInsensitive) {
  Database database;
  CommandExecutor executor(database);

  redis::CommandResult result = executor.Execute({"watch", "key"});

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(RespWriter::Write(*result), "+OK\r\n");
}

TEST(TransactionTest, WatchRequiresExactlyOneKey) {
  Database database;
  CommandExecutor executor(database);

  redis::CommandResult missing_key = executor.Execute({"WATCH"});
  ASSERT_FALSE(missing_key.has_value());
  EXPECT_EQ(missing_key.error().code, CommandErrorCode::kWrongArity);

  redis::CommandResult multiple_keys =
      executor.Execute({"WATCH", "key1", "key2"});
  ASSERT_FALSE(multiple_keys.has_value());
  EXPECT_EQ(multiple_keys.error().code, CommandErrorCode::kWrongArity);
}

}  // namespace
