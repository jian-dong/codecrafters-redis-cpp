#include "test_support.hpp"

namespace {

using redis::CommandExecutor;
using redis::Database;
using redis::RespArray;
using redis::RespWriter;

TEST(SortedSetRangeTest, ZrangeReturnsSortedSetMembersByIndex) {
  Database database;
  CommandExecutor executor(database, false);

  ASSERT_TRUE((executor.Execute({"ZADD", "zset_key", "100.0", "foo"}).has_value())) << "setup ZADD foo should succeed";
  ASSERT_TRUE((executor.Execute({"ZADD", "zset_key", "100.0", "bar"}).has_value())) << "setup ZADD bar should succeed";
  ASSERT_TRUE((executor.Execute({"ZADD", "zset_key", "20.0", "baz"}).has_value())) << "setup ZADD baz should succeed";
  ASSERT_TRUE((executor.Execute({"ZADD", "zset_key", "30.1", "caz"}).has_value())) << "setup ZADD caz should succeed";
  ASSERT_TRUE((executor.Execute({"ZADD", "zset_key", "40.2", "paz"}).has_value())) << "setup ZADD paz should succeed";

  redis::CommandResult result = executor.Execute({"ZRANGE", "zset_key", "2", "4"});
  ASSERT_TRUE((result.has_value())) << "ZRANGE 2 4 should succeed";
  ASSERT_TRUE((result->Is<RespArray>())) << "ZRANGE should return a RESP array";
  ASSERT_TRUE((RespBulkStrings(result->Get<RespArray>()) ==
             std::vector<std::string>({"paz", "bar", "foo"}))) << "ZRANGE should return members in sorted-set order";
  ASSERT_TRUE((RespWriter::Write(*result) ==
             "*3\r\n$3\r\npaz\r\n$3\r\nbar\r\n$3\r\nfoo\r\n")) << "ZRANGE should encode members as a RESP array";

  result = executor.Execute({"ZRANGE", "zset_key", "0", "10"});
  ASSERT_TRUE((result.has_value())) << "ZRANGE 0 10 should succeed";
  ASSERT_TRUE((result->Is<RespArray>())) << "ZRANGE with large stop should return a RESP array";
  ASSERT_TRUE((RespBulkStrings(result->Get<RespArray>()) ==
             std::vector<std::string>({"baz", "caz", "paz", "bar", "foo"}))) << "ZRANGE should clamp stop to the last member";

  result = executor.Execute({"ZRANGE", "zset_key", "2", "-1"});
  ASSERT_TRUE((result.has_value())) << "ZRANGE 2 -1 should succeed";
  ASSERT_TRUE((result->Is<RespArray>())) << "ZRANGE with a negative stop should return a RESP array";
  ASSERT_TRUE((RespBulkStrings(result->Get<RespArray>()) ==
             std::vector<std::string>({"paz", "bar", "foo"}))) << "ZRANGE should resolve -1 to the last member";

  result = executor.Execute({"ZRANGE", "zset_key", "0", "-3"});
  ASSERT_TRUE((result.has_value())) << "ZRANGE 0 -3 should succeed";
  ASSERT_TRUE((result->Is<RespArray>())) << "ZRANGE with a negative stop offset should return a RESP array";
  ASSERT_TRUE((RespBulkStrings(result->Get<RespArray>()) ==
             std::vector<std::string>({"baz", "caz", "paz"}))) << "ZRANGE should count negative indexes from the end";

  result = executor.Execute({"ZRANGE", "zset_key", "-2", "-1"});
  ASSERT_TRUE((result.has_value())) << "ZRANGE -2 -1 should succeed";
  ASSERT_TRUE((result->Is<RespArray>())) << "ZRANGE with negative start and stop should return a RESP array";
  ASSERT_TRUE((RespBulkStrings(result->Get<RespArray>()) ==
             std::vector<std::string>({"bar", "foo"}))) << "ZRANGE should resolve negative indexes from the tail";

  result = executor.Execute({"ZRANGE", "zset_key", "-99", "-1"});
  ASSERT_TRUE((result.has_value())) << "ZRANGE with out-of-range negative start should succeed";
  ASSERT_TRUE((result->Is<RespArray>())) << "ZRANGE with out-of-range negative start should return a RESP array";
  ASSERT_TRUE((RespBulkStrings(result->Get<RespArray>()) ==
             std::vector<std::string>({"baz", "caz", "paz", "bar", "foo"}))) << "ZRANGE should clamp out-of-range negative indexes to the start";

  result = executor.Execute({"ZRANGE", "zset_key", "5", "6"});
  ASSERT_TRUE((result.has_value())) << "ZRANGE past end should succeed";
  ASSERT_TRUE((result->Is<RespArray>())) << "ZRANGE past end should still return a RESP array";
  ASSERT_TRUE((result->Get<RespArray>().values.empty())) << "ZRANGE past end should return an empty array";
  ASSERT_TRUE((RespWriter::Write(*result) == "*0\r\n")) << "ZRANGE empty results should encode as an empty array";

  result = executor.Execute({"ZRANGE", "zset_key", "4", "2"});
  ASSERT_TRUE((result.has_value())) << "ZRANGE with start > stop should succeed";
  ASSERT_TRUE((result->Is<RespArray>())) << "ZRANGE with start > stop should return a RESP array";
  ASSERT_TRUE((result->Get<RespArray>().values.empty())) << "ZRANGE with start > stop should return an empty array";

  result = executor.Execute({"ZRANGE", "missing_key", "0", "1"});
  ASSERT_TRUE((result.has_value())) << "ZRANGE missing key should succeed";
  ASSERT_TRUE((result->Is<RespArray>())) << "ZRANGE missing key should return a RESP array";
  ASSERT_TRUE((result->Get<RespArray>().values.empty())) << "ZRANGE missing key should return an empty array";
}

}  // namespace
