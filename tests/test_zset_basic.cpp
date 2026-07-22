#include "test_support.hpp"

namespace {

using redis::CommandExecutor;
using redis::Database;
using redis::RespInteger;
using redis::RespWriter;

TEST(SortedSetBasicTest, ZaddCreatesSortedSetAndReturnsAddedCount) {
  Database database;
  CommandExecutor executor(database, false);

  redis::CommandResult result =
      executor.Execute({"ZADD", "zset_key", "10.0", "zset_member"});
  ASSERT_TRUE((result.has_value())) << "ZADD should succeed";
  ASSERT_TRUE((result->Is<RespInteger>())) << "ZADD should return a RESP integer";
  ASSERT_TRUE((result->Get<RespInteger>().value == 1)) << "ZADD should report one added member for a new sorted set";
  ASSERT_TRUE((RespWriter::Write(*result) == ":1\r\n")) << "ZADD should encode the added-member count as a RESP integer";
}

TEST(SortedSetBasicTest, ZrankReturnsSortedSetRankAndNilForMissingMembers) {
  Database database;
  CommandExecutor executor(database, false);

  ASSERT_TRUE((executor.Execute({"ZADD", "zset_key", "100.0", "foo"}).has_value())) << "setup ZADD foo should succeed";
  ASSERT_TRUE((executor.Execute({"ZADD", "zset_key", "100.0", "bar"}).has_value())) << "setup ZADD bar should succeed";
  ASSERT_TRUE((executor.Execute({"ZADD", "zset_key", "20.0", "baz"}).has_value())) << "setup ZADD baz should succeed";
  ASSERT_TRUE((executor.Execute({"ZADD", "zset_key", "30.1", "caz"}).has_value())) << "setup ZADD caz should succeed";
  ASSERT_TRUE((executor.Execute({"ZADD", "zset_key", "40.2", "paz"}).has_value())) << "setup ZADD paz should succeed";

  redis::CommandResult result = executor.Execute({"ZRANK", "zset_key", "caz"});
  ASSERT_TRUE((result.has_value())) << "ZRANK caz should succeed";
  ASSERT_TRUE((result->Is<RespInteger>())) << "ZRANK should return a RESP integer for existing members";
  ASSERT_TRUE((result->Get<RespInteger>().value == 1)) << "ZRANK should return the member rank in score order";
  ASSERT_TRUE((RespWriter::Write(*result) == ":1\r\n")) << "ZRANK should encode ranks as RESP integers";

  result = executor.Execute({"ZRANK", "zset_key", "bar"});
  ASSERT_TRUE((result.has_value())) << "ZRANK bar should succeed";
  ASSERT_TRUE((result->Is<RespInteger>())) << "ZRANK bar should return a RESP integer";
  ASSERT_TRUE((result->Get<RespInteger>().value == 3)) << "ZRANK should break score ties lexicographically";

  result = executor.Execute({"ZRANK", "zset_key", "missing_member"});
  ASSERT_TRUE((result.has_value())) << "ZRANK missing member should succeed";
  ASSERT_TRUE((result->Is<redis::RespNullBulk>())) << "ZRANK missing member should return a null bulk string";
  ASSERT_TRUE((RespWriter::Write(*result) == "$-1\r\n")) << "ZRANK missing member should encode as a null bulk string";

  result = executor.Execute({"ZRANK", "missing_key", "member"});
  ASSERT_TRUE((result.has_value())) << "ZRANK missing key should succeed";
  ASSERT_TRUE((result->Is<redis::RespNullBulk>())) << "ZRANK missing key should return a null bulk string";
}

}  // namespace
