#include "test_support.hpp"

#include <cstdint>
#include <limits>

namespace {

using redis::CommandErrorCode;
using redis::CommandExecutor;
using redis::Database;
using redis::DatabaseErrorCode;
using redis::ValueType;

void ExpectWrongType(const redis::DatabaseError &error, ValueType actual_type) {
  EXPECT_EQ(error.code, DatabaseErrorCode::kWrongType);
  ASSERT_TRUE(error.actual_type.has_value());
  EXPECT_EQ(*error.actual_type, actual_type);
}

TEST(DatabaseTest, RepresentsMissingKeysWithoutFailureSentinels) {
  Database database;

  const auto string = database.GetString("missing");
  ASSERT_TRUE(string.has_value());
  EXPECT_FALSE(string->has_value());

  const auto popped = database.PopLeft("missing");
  ASSERT_TRUE(popped.has_value());
  EXPECT_FALSE(popped->has_value());

  const auto rank = database.ZRank("missing", "member");
  ASSERT_TRUE(rank.has_value());
  EXPECT_FALSE(rank->has_value());

  const auto score = database.ZScore("missing", "member");
  ASSERT_TRUE(score.has_value());
  EXPECT_FALSE(score->has_value());

  const auto stream_cursor = database.LastStreamId("missing");
  ASSERT_TRUE(stream_cursor.has_value());
  EXPECT_FALSE(stream_cursor->has_value());

  const auto range = database.Range("missing", 0, -1);
  ASSERT_TRUE(range.has_value());
  EXPECT_TRUE(range->empty());
  const auto length = database.Length("missing");
  ASSERT_TRUE(length.has_value());
  EXPECT_EQ(*length, 0);
  EXPECT_FALSE(database.TypeOf("missing").has_value());
}

TEST(DatabaseTest, SaturatesExpirationThatExceedsSteadyClockRange) {
  Database database;

  database.SetString("long-lived", "value",
                     std::chrono::milliseconds::max());

  const auto value = database.GetString("long-lived");
  ASSERT_TRUE(value.has_value());
  ASSERT_TRUE(value->has_value());
  EXPECT_EQ(**value, "value");
}

TEST(DatabaseTest, ReportsActualTypeAcrossValueFamilies) {
  Database database;
  database.SetString("string", "value");

  const auto list_result = database.PushRight("string", {"item"});
  ASSERT_FALSE(list_result.has_value());
  ExpectWrongType(list_result.error(), ValueType::kString);

  const auto stream_result =
      database.XAdd("string", "1-0", {{"field", "value"}});
  ASSERT_FALSE(stream_result.has_value());
  ExpectWrongType(stream_result.error(), ValueType::kString);

  const auto sorted_set_result = database.ZAdd("string", 1.0, "1", "member");
  ASSERT_FALSE(sorted_set_result.has_value());
  ExpectWrongType(sorted_set_result.error(), ValueType::kString);

  ASSERT_TRUE(database.PushRight("list", {"item"}).has_value());
  const auto string_result = database.GetString("list");
  ASSERT_FALSE(string_result.has_value());
  ExpectWrongType(string_result.error(), ValueType::kList);

  const auto increment_result = database.Incr("list");
  ASSERT_FALSE(increment_result.has_value());
  ExpectWrongType(increment_result.error(), ValueType::kList);
}

TEST(DatabaseTest, IncrReturnsTypedErrorsWithoutMutatingState) {
  Database database;

  const auto first = database.Incr("counter");
  ASSERT_TRUE(first.has_value());
  EXPECT_EQ(*first, 1);
  const auto second = database.Incr("counter");
  ASSERT_TRUE(second.has_value());
  EXPECT_EQ(*second, 2);

  database.SetString("invalid", "not-an-integer");
  const uint64_t invalid_version = database.KeyVersion("invalid");
  const auto invalid = database.Incr("invalid");
  ASSERT_FALSE(invalid.has_value());
  EXPECT_EQ(invalid.error().code, DatabaseErrorCode::kInvalidInteger);
  EXPECT_EQ(database.KeyVersion("invalid"), invalid_version);
  const auto invalid_value = database.GetString("invalid");
  ASSERT_TRUE(invalid_value.has_value());
  ASSERT_TRUE(invalid_value->has_value());
  EXPECT_EQ(**invalid_value, "not-an-integer");

  const std::string maximum =
      std::to_string(std::numeric_limits<int64_t>::max());
  database.SetString("maximum", maximum);
  const uint64_t maximum_version = database.KeyVersion("maximum");
  const auto overflow = database.Incr("maximum");
  ASSERT_FALSE(overflow.has_value());
  EXPECT_EQ(overflow.error().code, DatabaseErrorCode::kIntegerOverflow);
  EXPECT_EQ(database.KeyVersion("maximum"), maximum_version);
  const auto maximum_value = database.GetString("maximum");
  ASSERT_TRUE(maximum_value.has_value());
  ASSERT_TRUE(maximum_value->has_value());
  EXPECT_EQ(**maximum_value, maximum);
}

TEST(DatabaseTest, StreamErrorsDoNotCreateOrMutateAKey) {
  Database database;

  const auto malformed =
      database.XAdd("events", "not-an-id", {{"field", "value"}});
  ASSERT_FALSE(malformed.has_value());
  EXPECT_EQ(malformed.error().code, DatabaseErrorCode::kInvalidStreamId);
  EXPECT_FALSE(database.TypeOf("events").has_value());

  const auto zero = database.XAdd("events", "0-0", {{"field", "value"}});
  ASSERT_FALSE(zero.has_value());
  EXPECT_EQ(zero.error().code,
            DatabaseErrorCode::kStreamIdNotGreaterThanZeroZero);
  EXPECT_FALSE(database.TypeOf("events").has_value());
  EXPECT_EQ(database.KeyVersion("events"), 0U);

  const auto added = database.XAdd("events", "1-0", {{"field", "value"}});
  ASSERT_TRUE(added.has_value());
  EXPECT_EQ(*added, "1-0");
  const uint64_t version = database.KeyVersion("events");

  const auto duplicate = database.XAdd("events", "1-0", {{"other", "value"}});
  ASSERT_FALSE(duplicate.has_value());
  EXPECT_EQ(duplicate.error().code,
            DatabaseErrorCode::kStreamIdNotGreaterThanTopItem);
  EXPECT_EQ(database.KeyVersion("events"), version);

  const auto invalid_range = database.XRange("events", "not-an-id", "+");
  ASSERT_FALSE(invalid_range.has_value());
  EXPECT_EQ(invalid_range.error().code, DatabaseErrorCode::kInvalidStreamId);
  EXPECT_EQ(database.KeyVersion("events"), version);

  const auto entries = database.XRange("events", "-", "+");
  ASSERT_TRUE(entries.has_value());
  ASSERT_EQ(entries->size(), 1U);
  EXPECT_EQ(entries->front().id, "1-0");
  EXPECT_EQ(
      entries->front().fields,
      (std::vector<std::pair<std::string, std::string>>{{"field", "value"}}));
}

TEST(DatabaseTest, StreamAutoSequenceRejectsOverflowWithoutMutation) {
  Database database;
  const std::string maximum_sequence =
      "1-" + std::to_string(std::numeric_limits<int64_t>::max());
  ASSERT_TRUE(database.XAdd("events", maximum_sequence, {{"field", "value"}})
                  .has_value());
  const uint64_t version = database.KeyVersion("events");

  const auto overflow = database.XAdd("events", "1-*", {{"other", "value"}});

  ASSERT_FALSE(overflow.has_value());
  EXPECT_EQ(overflow.error().code, DatabaseErrorCode::kInvalidStreamId);
  EXPECT_EQ(database.KeyVersion("events"), version);
  const auto entries = database.XRange("events", "-", "+");
  ASSERT_TRUE(entries.has_value());
  EXPECT_EQ(entries->size(), 1U);
}

TEST(DatabaseTest, StreamIdParsingSupportsAutomaticSequenceGeneration) {
  Database database;

  const auto first =
      database.XAdd("events", "42-0", {{"field", "first"}});
  ASSERT_TRUE(first.has_value());
  EXPECT_EQ(*first, "42-0");

  const auto generated =
      database.XAdd("events", "42-*", {{"field", "second"}});
  ASSERT_TRUE(generated.has_value());
  EXPECT_EQ(*generated, "42-1");

  const auto malformed =
      database.XAdd("events", "42-*-extra", {{"field", "third"}});
  ASSERT_FALSE(malformed.has_value());
  EXPECT_EQ(malformed.error().code, DatabaseErrorCode::kInvalidStreamId);

  const auto entries = database.XRange("events", "42", "42");
  ASSERT_TRUE(entries.has_value());
  ASSERT_EQ(entries->size(), 2U);
  EXPECT_EQ(entries->front().id, "42-0");
  EXPECT_EQ(entries->back().id, "42-1");
}

TEST(DatabaseTest, SortedSetRangeBeforeTheFirstMemberIsEmpty) {
  Database database;
  ASSERT_TRUE(database.ZAdd("scores", 1.0, "1", "one").has_value());
  ASSERT_TRUE(database.ZAdd("scores", 2.0, "2", "two").has_value());

  const auto members = database.ZRange("scores", 0, -99);

  ASSERT_TRUE(members.has_value());
  EXPECT_TRUE(members->empty());
}

TEST(DatabaseTest, WaitOperationsReturnExplicitOutcomes) {
  using namespace std::chrono_literals;

  Database database;
  EXPECT_EQ(database.WaitForListReady("missing", 1ms),
            redis::WaitOutcome::kTimedOut);
  ASSERT_TRUE(database.PushRight("ready", {"value"}).has_value());
  EXPECT_EQ(database.WaitForListReady("ready", 1ms),
            redis::WaitOutcome::kReady);

  const uint64_t generation = database.StreamGeneration();
  EXPECT_EQ(database.WaitForStreamChange(generation, 1ms),
            redis::WaitOutcome::kTimedOut);
}

TEST(CommandExecutorTest, IncrMapsDatabaseFailuresToRedisErrors) {
  Database database;
  CommandExecutor executor(database);

  const std::string maximum =
      std::to_string(std::numeric_limits<int64_t>::max());
  ASSERT_TRUE(executor.Execute({"SET", "counter", maximum}).has_value());
  const uint64_t version = database.KeyVersion("counter");

  const redis::CommandResult overflow = executor.Execute({"INCR", "counter"});

  ASSERT_FALSE(overflow.has_value());
  EXPECT_EQ(overflow.error().code, CommandErrorCode::kInvalidInteger);
  EXPECT_EQ(
      redis::RespWriter::Error(redis::CommandErrorMessage(overflow.error())),
      "-ERR value is not an integer or out of range\r\n");
  EXPECT_EQ(database.KeyVersion("counter"), version);

  ASSERT_TRUE(executor.Execute({"SET", "invalid", "text"}).has_value());
  const redis::CommandResult invalid = executor.Execute({"INCR", "invalid"});
  ASSERT_FALSE(invalid.has_value());
  EXPECT_EQ(invalid.error().code, CommandErrorCode::kInvalidInteger);

  ASSERT_TRUE(executor.Execute({"RPUSH", "items", "value"}).has_value());
  const redis::CommandResult wrong_type = executor.Execute({"INCR", "items"});
  ASSERT_FALSE(wrong_type.has_value());
  EXPECT_EQ(wrong_type.error().code, CommandErrorCode::kWrongType);
}

} // namespace
