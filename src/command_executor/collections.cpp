#include "redis-cpp/command_executor.hpp"

#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace redis {
namespace {

bool ParseDouble(std::string_view data, double& value) {
  if (data.empty()) {
    return false;
  }

  errno = 0;
  char* parse_end = nullptr;
  const std::string text(data);
  value = std::strtod(text.c_str(), &parse_end);
  return parse_end == text.c_str() + text.size() && errno != ERANGE;
}

std::string EncodeStreamRange(
    const std::vector<Database::StreamRangeEntry>& entries) {
  std::string encoded = "*" + std::to_string(entries.size()) + "\r\n";
  for (const Database::StreamRangeEntry& entry : entries) {
    encoded += "*2\r\n";
    encoded += RespWriter::Write(RespBulkString{entry.id});
    encoded += "*" + std::to_string(entry.values.size()) + "\r\n";
    for (const std::string& value : entry.values) {
      encoded += RespWriter::Write(RespBulkString{value});
    }
  }
  return encoded;
}

std::string EncodeXreadResponse(
    const std::vector<std::pair<std::string, std::vector<Database::StreamRangeEntry>>>&
        streams) {
  std::string encoded = "*" + std::to_string(streams.size()) + "\r\n";
  for (const auto& [key, entries] : streams) {
    encoded += "*2\r\n";
    encoded += RespWriter::Write(RespBulkString{key});
    encoded += EncodeStreamRange(entries);
  }
  return encoded;
}

}  // namespace

CommandResult CommandExecutor::HandleZadd(const std::vector<std::string>& args) {
  if (args.size() != 4) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "zadd"});
  }

  double score = 0.0;
  if (!ParseDouble(args[2], score)) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kSyntaxError, .command = "zadd"});
  }

  const Database::ZAddResult result =
      database_.ZAdd(args[1], score, args[2], args[3]);
  if (result.wrong_type) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongType, .command = "zadd"});
  }

  return RespInteger{result.added};
}

CommandResult CommandExecutor::HandleZrank(
    const std::vector<std::string>& args) {
  if (args.size() != 3) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "zrank"});
  }

  const Database::ZRankResult result = database_.ZRank(args[1], args[2]);
  if (result.wrong_type) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongType, .command = "zrank"});
  }
  if (!result.found) {
    return RespNullBulk{};
  }

  return RespInteger{result.rank};
}

CommandResult CommandExecutor::HandleZrange(
    const std::vector<std::string>& args) {
  if (args.size() != 4) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "zrange"});
  }

  int64_t start = 0;
  int64_t stop = 0;
  if (!ParseSignedInteger(args[2], start) ||
      !ParseSignedInteger(args[3], stop)) {
    return RespArray{};
  }

  const Database::ZRangeResult result = database_.ZRange(args[1], start, stop);
  if (result.wrong_type) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongType, .command = "zrange"});
  }

  return RespArray{result.members};
}

CommandResult CommandExecutor::HandleZcard(
    const std::vector<std::string>& args) {
  if (args.size() != 2) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "zcard"});
  }

  const Database::ZCardResult result = database_.ZCard(args[1]);
  if (result.wrong_type) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongType, .command = "zcard"});
  }

  return RespInteger{result.cardinality};
}

CommandResult CommandExecutor::HandleZscore(
    const std::vector<std::string>& args) {
  if (args.size() != 3) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "zscore"});
  }

  const Database::ZScoreResult result = database_.ZScore(args[1], args[2]);
  if (result.wrong_type) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongType, .command = "zscore"});
  }
  if (!result.found) {
    return RespNullBulk{};
  }

  return RespBulkString{result.score};
}

CommandResult CommandExecutor::HandleZrem(const std::vector<std::string>& args) {
  if (args.size() != 3) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "zrem"});
  }

  const Database::ZRemResult result = database_.ZRem(args[1], args[2]);
  if (result.wrong_type) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongType, .command = "zrem"});
  }

  return RespInteger{result.removed};
}

CommandResult CommandExecutor::HandleXadd(const std::vector<std::string>& args) {
  if (args.size() < 5 || args.size() % 2 == 0) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "xadd"});
  }

  std::vector<std::pair<std::string, std::string>> fields;
  fields.reserve((args.size() - 3) / 2);
  for (size_t index = 3; index < args.size(); index += 2) {
    fields.emplace_back(args[index], args[index + 1]);
  }

  const Database::StreamAddResult result =
      database_.XAdd(args[1], args[2], fields);
  switch (result.status) {
    case Database::StreamAddResult::Status::kOk:
      return RespBulkString{result.id};
    case Database::StreamAddResult::Status::kWrongType:
      return tl::make_unexpected(
          CommandError{.code = CommandErrorCode::kWrongType, .command = "xadd"});
    case Database::StreamAddResult::Status::kIdNotGreaterThanZeroZero:
      return tl::make_unexpected(CommandError{
          .code = CommandErrorCode::kXaddIdNotGreaterThanZeroZero,
          .command = "xadd"});
    case Database::StreamAddResult::Status::kIdNotGreaterThanTopItem:
      return tl::make_unexpected(CommandError{
          .code = CommandErrorCode::kXaddIdNotGreaterThanTopItem,
          .command = "xadd"});
    case Database::StreamAddResult::Status::kInvalidId:
      return tl::make_unexpected(
          CommandError{.code = CommandErrorCode::kSyntaxError, .command = "xadd"});
  }

  return tl::make_unexpected(
      CommandError{.code = CommandErrorCode::kSyntaxError, .command = "xadd"});
}

CommandResult CommandExecutor::HandleXrange(
    const std::vector<std::string>& args) {
  if (args.size() != 4) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kWrongArity, .command = "xrange"});
  }

  const Database::StreamRangeResult result =
      database_.XRange(args[1], args[2], args[3]);
  if (result.wrong_type) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kWrongType, .command = "xrange"});
  }
  if (result.invalid_id) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kSyntaxError, .command = "xrange"});
  }

  return RespRaw{EncodeStreamRange(result.entries)};
}

CommandResult CommandExecutor::HandleXread(
    const std::vector<std::string>& args) {
  if (args.size() < 4) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kWrongArity, .command = "xread"});
  }

  size_t streams_index = 1;
  std::optional<std::chrono::steady_clock::duration> block_timeout;
  if (ToUpperAscii(args[streams_index]) == "BLOCK") {
    if (args.size() < 6) {
      return tl::make_unexpected(CommandError{
          .code = CommandErrorCode::kWrongArity, .command = "xread"});
    }

    int64_t timeout_milliseconds = 0;
    if (!ParseMilliseconds(args[streams_index + 1], timeout_milliseconds)) {
      return tl::make_unexpected(CommandError{
          .code = CommandErrorCode::kInvalidInteger, .command = "xread"});
    }

    block_timeout = std::chrono::milliseconds(timeout_milliseconds);
    streams_index += 2;
  }

  if (streams_index >= args.size() ||
      ToUpperAscii(args[streams_index]) != "STREAMS" ||
      args.size() <= streams_index + 2 ||
      (args.size() - streams_index - 1) % 2 != 0) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kWrongArity, .command = "xread"});
  }

  const size_t stream_count = (args.size() - streams_index - 1) / 2;
  std::vector<std::pair<std::string, std::string>> stream_specs;
  stream_specs.reserve(stream_count);
  for (size_t index = 0; index < stream_count; ++index) {
    stream_specs.emplace_back(args[streams_index + 1 + index],
                              args[streams_index + 1 + stream_count + index]);
  }

  if (block_timeout.has_value()) {
    const Database::BlockingStreamReadResult result =
        database_.BlockingXRead(stream_specs, *block_timeout);
    if (result.wrong_type) {
      return tl::make_unexpected(CommandError{
          .code = CommandErrorCode::kWrongType, .command = "xread"});
    }
    if (result.invalid_id) {
      return tl::make_unexpected(CommandError{
          .code = CommandErrorCode::kSyntaxError, .command = "xread"});
    }
    if (result.streams.empty()) {
      return RespNullArray{};
    }

    return RespRaw{EncodeXreadResponse(result.streams)};
  }

  std::vector<std::pair<std::string, std::vector<Database::StreamRangeEntry>>>
      streams;
  streams.reserve(stream_count);

  for (size_t index = 0; index < stream_count; ++index) {
    const Database::StreamRangeResult result =
        database_.XRead(stream_specs[index].first, stream_specs[index].second);
    if (result.wrong_type) {
      return tl::make_unexpected(CommandError{
          .code = CommandErrorCode::kWrongType, .command = "xread"});
    }
    if (result.invalid_id) {
      return tl::make_unexpected(CommandError{
          .code = CommandErrorCode::kSyntaxError, .command = "xread"});
    }
    if (!result.entries.empty()) {
      streams.emplace_back(stream_specs[index].first, result.entries);
    }
  }

  if (streams.empty()) {
    return RespNullArray{};
  }

  return RespRaw{EncodeXreadResponse(streams)};
}

CommandResult CommandExecutor::HandleRpush(
    const std::vector<std::string>& args) {
  if (args.size() < 3) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kWrongArity, .command = "rpush"});
  }

  const std::vector<std::string> values(args.begin() + 2, args.end());
  const Database::ListMutationResult result =
      database_.PushRight(args[1], values);
  if (result.wrong_type) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongType, .command = "rpush"});
  }

  return RespInteger{result.size};
}

CommandResult CommandExecutor::HandleLpush(
    const std::vector<std::string>& args) {
  if (args.size() < 3) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kWrongArity, .command = "lpush"});
  }

  const std::vector<std::string> values(args.begin() + 2, args.end());
  const Database::ListMutationResult result =
      database_.PushLeft(args[1], values);
  if (result.wrong_type) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongType, .command = "lpush"});
  }

  return RespInteger{result.size};
}

CommandResult CommandExecutor::HandleLrange(
    const std::vector<std::string>& args) {
  if (args.size() < 4) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kWrongArity, .command = "lrange"});
  }

  int64_t start = 0;
  int64_t stop = 0;
  if (!ParseSignedInteger(args[2], start) ||
      !ParseSignedInteger(args[3], stop)) {
    return RespArray{};
  }

  const Database::ListRangeResult result =
      database_.Range(args[1], start, stop);
  if (result.wrong_type) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kWrongType, .command = "lrange"});
  }

  return RespArray{result.values};
}

CommandResult CommandExecutor::HandleLlen(const std::vector<std::string>& args) {
  if (args.size() < 2) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "llen"});
  }

  const Database::ListLengthResult result = database_.Length(args[1]);
  if (result.wrong_type) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongType, .command = "llen"});
  }

  return RespInteger{result.length};
}

CommandResult CommandExecutor::HandleLpop(const std::vector<std::string>& args) {
  if (args.size() < 2) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "lpop"});
  }

  if (args.size() >= 3) {
    int64_t count = 0;
    if (!ParseSignedInteger(args[2], count) || count <= 0) {
      return RespArray{};
    }

    const Database::ListPopManyResult result =
        database_.PopLeft(args[1], static_cast<size_t>(count));
    if (result.wrong_type) {
      return tl::make_unexpected(CommandError{
          .code = CommandErrorCode::kWrongType, .command = "lpop"});
    }

    return RespArray{result.values};
  }

  const Database::ListPopResult result = database_.PopLeft(args[1]);
  if (result.wrong_type) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongType, .command = "lpop"});
  }
  if (!result.found) {
    return RespNullBulk{};
  }

  return RespBulkString{result.value};
}

CommandResult CommandExecutor::HandleBlpop(
    const std::vector<std::string>& args) {
  if (args.size() < 3) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kWrongArity, .command = "blpop"});
  }

  std::chrono::steady_clock::duration timeout{};
  if (!ParseTimeoutDuration(args[2], timeout)) {
    return RespNullArray{};
  }

  const Database::BlockingPopResult result =
      database_.BlockingPopLeft(args[1], timeout);
  if (result.wrong_type) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongType, .command = "blpop"});
  }
  if (!result.found) {
    return RespNullArray{};
  }

  return RespArray{{result.key, result.value}};
}

}  // namespace redis
