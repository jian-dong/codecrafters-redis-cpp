#include "test_support.hpp"

#include <type_traits>

namespace {

template <typename T>
concept HasPublicRespStorage = requires(T value) { value.storage_; };

static_assert(!std::is_constructible_v<redis::RespArray,
                                       std::vector<std::string>>);
static_assert(
    !std::is_base_of_v<redis::RespValueStorage, redis::RespValue>);
static_assert(!std::is_convertible_v<const redis::RespValue&,
                                     const redis::RespValueStorage&>);
static_assert(!std::is_constructible_v<redis::RespValue,
                                       redis::RespValueStorage>);
static_assert(!HasPublicRespStorage<redis::RespValue>);

TEST(RespArrayTest, BulkStringsBuildsExplicitBulkStringElements) {
  const redis::RespArray array =
      redis::RespArray::BulkStrings({"pong", ""});

  ASSERT_EQ(array.values.size(), 2U);
  ASSERT_TRUE(array.values[0].Is<redis::RespBulkString>());
  ASSERT_TRUE(array.values[1].Is<redis::RespBulkString>());
  EXPECT_EQ(array.values[0].Get<redis::RespBulkString>().value, "pong");
  EXPECT_TRUE(array.values[1].Get<redis::RespBulkString>().value.empty());
}

TEST(RespWriterTest, EncodesNestedStructuredValues) {
  const redis::RespValue response = redis::RespArray{
      std::vector<redis::RespValue>{
          redis::RespBulkString{"flags"},
          redis::RespArray::BulkStrings({"nopass"}),
          redis::RespInteger{2},
          redis::RespNullArray{}}};

  EXPECT_EQ(redis::RespWriter::Write(response),
            "*4\r\n$5\r\nflags\r\n*1\r\n$6\r\nnopass\r\n:2\r\n*-1\r\n");
}

TEST(RespWriterTest, EncodesErrorsThroughTheValueModel) {
  const redis::RespValue error = redis::RespErrorReply{"ERR failed"};

  EXPECT_EQ(redis::RespWriter::Write(error), "-ERR failed\r\n");
  EXPECT_EQ(redis::RespWriter::Error("ERR failed"), "-ERR failed\r\n");
}

TEST(RespWriterTest, LineResponsesCannotInjectAdditionalFrames) {
  EXPECT_EQ(redis::RespWriter::Write(
                redis::RespErrorReply{"ERR bad\r\n+INJECTED"}),
            "-ERR bad  +INJECTED\r\n");
  EXPECT_EQ(redis::RespWriter::Write(
                redis::RespSimpleString{"OK\n+INJECTED"}),
            "+OK +INJECTED\r\n");
}

TEST(RespWriterTest, EncodesSequencesAndFileTransfers) {
  const redis::RespValue response = redis::RespSequence{
      redis::RespSimpleString{"FULLRESYNC id 0"},
      redis::RespFileTransfer{"RDB"}};

  EXPECT_EQ(redis::RespWriter::Write(response),
            "+FULLRESYNC id 0\r\n$3\r\nRDB");
}

TEST(RespWriterTest, EncodesCommandsAsBulkStringArrays) {
  EXPECT_EQ(redis::RespWriter::WriteCommand({"SET", "foo", "100"}),
            "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n100\r\n");
}

TEST(RespParserTest, ParsesEveryConcatenatedCommand) {
  redis::RespParser parser;
  parser.Append(redis::RespWriter::WriteCommand({"SET", "foo", "100"}) +
                redis::RespWriter::WriteCommand({"GET", "foo"}));

  auto first = parser.NextCommand();
  auto second = parser.NextCommand();
  auto done = parser.NextCommand();

  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(second.has_value());
  ASSERT_TRUE(done.has_value());
  ASSERT_TRUE(first->has_value());
  ASSERT_TRUE(second->has_value());
  EXPECT_EQ(**first, std::vector<std::string>({"SET", "foo", "100"}));
  EXPECT_EQ(**second, std::vector<std::string>({"GET", "foo"}));
  EXPECT_FALSE(done->has_value());
}

TEST(RespParserTest, PreservesIncompleteSuffixWhenAppendingMoreData) {
  redis::RespParser parser(redis::RespParser::InputMode::kRespOnly);
  parser.Append("*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$5\r\nhel");

  const auto first = parser.NextCommand();
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(first->has_value());
  EXPECT_EQ(**first, std::vector<std::string>({"PING"}));

  const auto incomplete = parser.NextCommand();
  ASSERT_TRUE(incomplete.has_value());
  EXPECT_FALSE(incomplete->has_value());

  parser.Append("lo\r\n");
  const auto second = parser.NextCommand();
  ASSERT_TRUE(second.has_value());
  ASSERT_TRUE(second->has_value());
  EXPECT_EQ(**second, std::vector<std::string>({"ECHO", "hello"}));
  EXPECT_EQ(parser.BufferSize(), 0U);
}

TEST(RespParserTest, RejectsMalformedFrames) {
  redis::RespParser parser;
  parser.Append("*1\r\n+PING\r\n");

  const auto result = parser.NextCommand();

  ASSERT_FALSE(result.has_value());
  const auto* error = std::get_if<redis::RespError>(&result.error().Kind());
  ASSERT_NE(error, nullptr);
  EXPECT_EQ(error->code, redis::RespErrorCode::kInvalidFrame);
}

TEST(RespParserTest, RejectsArrayDeclarationsBeyondItsResourceBudget) {
  redis::RespParser parser(redis::RespParser::InputMode::kRespOnly);
  parser.Append("*2147483647\r\n");

  const auto result = parser.NextCommand();

  ASSERT_FALSE(result.has_value());
  const auto* error = std::get_if<redis::RespError>(&result.error().Kind());
  ASSERT_NE(error, nullptr);
  EXPECT_EQ(error->code, redis::RespErrorCode::kFrameTooLarge);
}

TEST(RespParserTest, RejectsBulkDeclarationsBeyondItsResourceBudget) {
  redis::RespParser parser(redis::RespParser::InputMode::kRespOnly);
  parser.Append("*1\r\n$2147483647\r\n");

  const auto result = parser.NextCommand();

  ASSERT_FALSE(result.has_value());
  const auto* error = std::get_if<redis::RespError>(&result.error().Kind());
  ASSERT_NE(error, nullptr);
  EXPECT_EQ(error->code, redis::RespErrorCode::kFrameTooLarge);
}

TEST(RespParserTest, ParsesInlineClientCommandsWithoutInventingPing) {
  redis::RespParser parser;
  parser.Append("ECHO hello\r\n");

  const auto command = parser.NextCommand();
  ASSERT_TRUE(command.has_value());
  ASSERT_TRUE(command->has_value());
  EXPECT_EQ(**command, (std::vector<std::string>{"ECHO", "hello"}));
}

TEST(RespParserTest, RespOnlyModeRejectsInlineInput) {
  redis::RespParser parser(redis::RespParser::InputMode::kRespOnly);
  parser.Append("PING\r\n");

  const auto command = parser.NextCommand();
  EXPECT_FALSE(command.has_value());
}

TEST(RespParserTest, RejectsEmptyCommandArrays) {
  redis::RespParser parser(redis::RespParser::InputMode::kRespOnly);
  parser.Append("*0\r\n");

  EXPECT_FALSE(parser.NextCommand().has_value());
}

}  // namespace
