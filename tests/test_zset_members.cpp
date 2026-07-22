#include "test_support.hpp"

namespace {

using redis::CommandExecutor;
using redis::Database;
using redis::RespInteger;
using redis::RespWriter;

TEST(SortedSetMembersTest, ZcardReturnsSortedSetCardinality) {
  Database database;
  CommandExecutor executor(database, false);

  ASSERT_TRUE((executor.Execute({"ZADD", "zset_key", "20.0", "zset_member1"}).has_value())) << "setup ZADD zset_member1 should succeed";
  ASSERT_TRUE((executor.Execute({"ZADD", "zset_key", "30.1", "zset_member2"}).has_value())) << "setup ZADD zset_member2 should succeed";
  ASSERT_TRUE((executor.Execute({"ZADD", "zset_key", "40.2", "zset_member3"}).has_value())) << "setup ZADD zset_member3 should succeed";
  ASSERT_TRUE((executor.Execute({"ZADD", "zset_key", "50.3", "zset_member4"}).has_value())) << "setup ZADD zset_member4 should succeed";

  redis::CommandResult result = executor.Execute({"ZCARD", "zset_key"});
  ASSERT_TRUE((result.has_value())) << "ZCARD should succeed";
  ASSERT_TRUE((result->Is<RespInteger>())) << "ZCARD should return a RESP integer";
  ASSERT_TRUE((result->Get<RespInteger>().value == 4)) << "ZCARD should return the number of members in the sorted set";
  ASSERT_TRUE((RespWriter::Write(*result) == ":4\r\n")) << "ZCARD should encode the cardinality as a RESP integer";

  result = executor.Execute({"ZADD", "zset_key", "100.0", "zset_member1"});
  ASSERT_TRUE((result.has_value())) << "updating an existing member should succeed";
  ASSERT_TRUE((result->Is<RespInteger>())) << "updating an existing member should return a RESP integer";
  ASSERT_TRUE((result->Get<RespInteger>().value == 0)) << "updating an existing member should not change cardinality";

  result = executor.Execute({"ZCARD", "zset_key"});
  ASSERT_TRUE((result.has_value())) << "ZCARD after update should succeed";
  ASSERT_TRUE((result->Is<RespInteger>())) << "ZCARD after update should return a RESP integer";
  ASSERT_TRUE((result->Get<RespInteger>().value == 4)) << "ZCARD should remain unchanged after updating an existing member";

  result = executor.Execute({"ZCARD", "missing_key"});
  ASSERT_TRUE((result.has_value())) << "ZCARD missing key should succeed";
  ASSERT_TRUE((result->Is<RespInteger>())) << "ZCARD missing key should return a RESP integer";
  ASSERT_TRUE((result->Get<RespInteger>().value == 0)) << "ZCARD missing key should return zero";
}

TEST(SortedSetMembersTest, ZscoreReturnsSortedSetMemberScore) {
  Database database;
  CommandExecutor executor(database, false);

  ASSERT_TRUE((executor.Execute({"ZADD", "zset_key", "20.0", "zset_member1"}).has_value())) << "setup ZADD zset_member1 should succeed";
  ASSERT_TRUE((executor.Execute({"ZADD", "zset_key", "30.1", "zset_member2"}).has_value())) << "setup ZADD zset_member2 should succeed";
  ASSERT_TRUE((executor.Execute({"ZADD", "zset_key", "40.2", "zset_member3"}).has_value())) << "setup ZADD zset_member3 should succeed";
  ASSERT_TRUE((executor.Execute({"ZADD", "zset_key", "50.3", "zset_member4"}).has_value())) << "setup ZADD zset_member4 should succeed";

  redis::CommandResult result = executor.Execute({"ZSCORE", "zset_key", "zset_member2"});
  ASSERT_TRUE((result.has_value())) << "ZSCORE should succeed";
  ASSERT_TRUE((result->Is<redis::RespBulkString>())) << "ZSCORE should return a RESP bulk string";
  ASSERT_TRUE((result->Get<redis::RespBulkString>().value == "30.1")) << "ZSCORE should return the member score";
  ASSERT_TRUE((RespWriter::Write(*result) == "$4\r\n30.1\r\n")) << "ZSCORE should encode the score as a RESP bulk string";

  result = executor.Execute({"ZADD", "zset_key", "100.99", "zset_member2"});
  ASSERT_TRUE((result.has_value())) << "updating a sorted-set member should succeed";
  ASSERT_TRUE((result->Is<RespInteger>())) << "updating a sorted-set member should return a RESP integer";
  ASSERT_TRUE((result->Get<RespInteger>().value == 0)) << "updating a sorted-set member should not add a new entry";

  result = executor.Execute({"ZSCORE", "zset_key", "zset_member2"});
  ASSERT_TRUE((result.has_value())) << "ZSCORE after update should succeed";
  ASSERT_TRUE((result->Is<redis::RespBulkString>())) << "ZSCORE after update should return a RESP bulk string";
  ASSERT_TRUE((result->Get<redis::RespBulkString>().value == "100.99")) << "ZSCORE should return the updated score";

  result = executor.Execute({"ZSCORE", "zset_key", "zset_member100"});
  ASSERT_TRUE((result.has_value())) << "ZSCORE missing member should succeed";
  ASSERT_TRUE((result->Is<redis::RespNullBulk>())) << "ZSCORE missing member should return a null bulk string";
  ASSERT_TRUE((RespWriter::Write(*result) == "$-1\r\n")) << "ZSCORE missing member should encode as a null bulk string";

  result = executor.Execute({"ZSCORE", "missing_key", "member"});
  ASSERT_TRUE((result.has_value())) << "ZSCORE missing key should succeed";
  ASSERT_TRUE((result->Is<redis::RespNullBulk>())) << "ZSCORE missing key should return a null bulk string";
}

TEST(SortedSetMembersTest, ZremRemovesSortedSetMember) {
  Database database;
  CommandExecutor executor(database, false);

  ASSERT_TRUE((executor.Execute({"ZADD", "zset_key", "80.5", "foo"}).has_value())) << "setup ZADD foo should succeed";
  ASSERT_TRUE((executor.Execute({"ZADD", "zset_key", "50.3", "baz"}).has_value())) << "setup ZADD baz should succeed";
  ASSERT_TRUE((executor.Execute({"ZADD", "zset_key", "80.5", "bar"}).has_value())) << "setup ZADD bar should succeed";

  redis::CommandResult result = executor.Execute({"ZREM", "zset_key", "baz"});
  ASSERT_TRUE((result.has_value())) << "ZREM baz should succeed";
  ASSERT_TRUE((result->Is<RespInteger>())) << "ZREM should return a RESP integer";
  ASSERT_TRUE((result->Get<RespInteger>().value == 1)) << "ZREM should report one removed member";
  ASSERT_TRUE((RespWriter::Write(*result) == ":1\r\n")) << "ZREM should encode the removed count as a RESP integer";

  result = executor.Execute({"ZRANGE", "zset_key", "0", "-1"});
  ASSERT_TRUE((result.has_value())) << "ZRANGE after ZREM should succeed";
  ASSERT_TRUE((result->Is<redis::RespArray>())) << "ZRANGE after ZREM should return a RESP array";
  ASSERT_TRUE((RespBulkStrings(result->Get<redis::RespArray>()) ==
             std::vector<std::string>({"bar", "foo"}))) << "ZREM should remove the member and keep the remaining sorted order";

  result = executor.Execute({"ZREM", "zset_key", "missing_member"});
  ASSERT_TRUE((result.has_value())) << "ZREM missing member should succeed";
  ASSERT_TRUE((result->Is<RespInteger>())) << "ZREM missing member should return a RESP integer";
  ASSERT_TRUE((result->Get<RespInteger>().value == 0)) << "ZREM missing member should report zero removals";
}

}  // namespace
