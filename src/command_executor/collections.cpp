#include "redis-cpp/command_executor.hpp"

#include <chrono>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "redis-cpp/numeric_parser.hpp"

namespace redis {
namespace {

std::chrono::steady_clock::duration SaturatingMilliseconds(
    int64_t milliseconds) {
  using Duration = std::chrono::steady_clock::duration;

  const auto maximum_milliseconds =
      std::chrono::duration_cast<std::chrono::milliseconds>(Duration::max());
  const auto parsed = std::chrono::milliseconds(milliseconds);
  if (parsed > maximum_milliseconds) {
    return Duration::max();
  }
  return std::chrono::duration_cast<Duration>(parsed);
}

std::chrono::steady_clock::time_point SaturatingDeadline(
    std::chrono::steady_clock::duration timeout) {
  using Clock = std::chrono::steady_clock;

  const Clock::time_point now = Clock::now();
  if (timeout > Clock::time_point::max() - now) {
    return Clock::time_point::max();
  }
  return now + timeout;
}

RespArray StreamRangeResponse(
    const std::vector<Database::StreamRangeEntry>& entries) {
  std::vector<RespValue> response;
  response.reserve(entries.size());
  for (const Database::StreamRangeEntry& entry : entries) {
    std::vector<std::string> fields;
    fields.reserve(entry.fields.size() * 2);
    for (const auto& [field, value] : entry.fields) {
      fields.push_back(field);
      fields.push_back(value);
    }
    response.emplace_back(RespArray{std::vector<RespValue>{
        RespBulkString{entry.id}, RespArray::BulkStrings(fields)}});
  }
  return RespArray{std::move(response)};
}

RespArray XreadResponse(
    const std::vector<std::pair<std::string, std::vector<Database::StreamRangeEntry>>>&
        streams) {
  std::vector<RespValue> response;
  response.reserve(streams.size());
  for (const auto& [key, entries] : streams) {
    response.emplace_back(RespArray{std::vector<RespValue>{
        RespBulkString{key}, StreamRangeResponse(entries)}});
  }
  return RespArray{std::move(response)};
}

}  // namespace

CommandResult CommandExecutor::HandleZadd(const std::vector<std::string>& args) {
  if (args.size() != 4) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "zadd"});
  }

  const std::optional<double> score = ParseFiniteDouble(args[2]);
  if (!score.has_value()) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kSyntaxError, .command = "zadd"});
  }

  const DatabaseResult<int64_t> result =
      database_.ZAdd(args[1], *score, args[2], args[3]);
  if (!result) {
    return tl::make_unexpected(MapDatabaseError(result.error(), "zadd"));
  }

  return RespInteger{*result};
}

CommandResult CommandExecutor::HandleZrank(
    const std::vector<std::string>& args) {
  if (args.size() != 3) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "zrank"});
  }

  const DatabaseResult<std::optional<int64_t>> result =
      database_.ZRank(args[1], args[2]);
  if (!result) {
    return tl::make_unexpected(MapDatabaseError(result.error(), "zrank"));
  }
  if (!result->has_value()) {
    return RespNullBulk{};
  }

  return RespInteger{**result};
}

CommandResult CommandExecutor::HandleZrange(
    const std::vector<std::string>& args) {
  if (args.size() != 4) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "zrange"});
  }

  const std::optional<int64_t> start = ParseSignedInteger(args[2]);
  const std::optional<int64_t> stop = ParseSignedInteger(args[3]);
  if (!start.has_value() || !stop.has_value()) {
    return RespArray{};
  }

  const DatabaseResult<std::vector<std::string>> result =
      database_.ZRange(args[1], *start, *stop);
  if (!result) {
    return tl::make_unexpected(MapDatabaseError(result.error(), "zrange"));
  }

  return RespArray::BulkStrings(*result);
}

CommandResult CommandExecutor::HandleZcard(
    const std::vector<std::string>& args) {
  if (args.size() != 2) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "zcard"});
  }

  const DatabaseResult<int64_t> result = database_.ZCard(args[1]);
  if (!result) {
    return tl::make_unexpected(MapDatabaseError(result.error(), "zcard"));
  }

  return RespInteger{*result};
}

CommandResult CommandExecutor::HandleZscore(
    const std::vector<std::string>& args) {
  if (args.size() != 3) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "zscore"});
  }

  const DatabaseResult<std::optional<std::string>> result =
      database_.ZScore(args[1], args[2]);
  if (!result) {
    return tl::make_unexpected(MapDatabaseError(result.error(), "zscore"));
  }
  if (!result->has_value()) {
    return RespNullBulk{};
  }

  return RespBulkString{**result};
}

CommandResult CommandExecutor::HandleZrem(const std::vector<std::string>& args) {
  if (args.size() != 3) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "zrem"});
  }

  const DatabaseResult<int64_t> result = database_.ZRem(args[1], args[2]);
  if (!result) {
    return tl::make_unexpected(MapDatabaseError(result.error(), "zrem"));
  }

  return RespInteger{*result};
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

  const DatabaseResult<std::string> result =
      database_.XAdd(args[1], args[2], fields);
  if (!result) {
    return tl::make_unexpected(MapDatabaseError(result.error(), "xadd"));
  }

  return RespBulkString{*result};
}

CommandResult CommandExecutor::HandleXrange(
    const std::vector<std::string>& args) {
  if (args.size() != 4) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kWrongArity, .command = "xrange"});
  }

  const DatabaseResult<std::vector<Database::StreamRangeEntry>> result =
      database_.XRange(args[1], args[2], args[3]);
  if (!result) {
    return tl::make_unexpected(MapDatabaseError(result.error(), "xrange"));
  }

  return StreamRangeResponse(*result);
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

    const std::optional<int64_t> timeout_milliseconds =
        ParseNonNegativeInteger(args[streams_index + 1]);
    if (!timeout_milliseconds.has_value()) {
      return tl::make_unexpected(CommandError{
          .code = CommandErrorCode::kInvalidInteger, .command = "xread"});
    }

    block_timeout = SaturatingMilliseconds(*timeout_milliseconds);
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
    std::lock_guard<std::recursive_mutex> execution_lock(transaction_mutex_);
    for (auto& [key, start] : stream_specs) {
      if (start != "$") {
        continue;
      }
      const DatabaseResult<std::optional<std::string>> cursor =
          database_.LastStreamId(key);
      if (!cursor) {
        return tl::make_unexpected(
            MapDatabaseError(cursor.error(), "xread"));
      }
      start = cursor->value_or("0-0");
    }
  }

  auto read_streams = [&]() -> CommandResult {
    std::vector<
        std::pair<std::string, std::vector<Database::StreamRangeEntry>>>
        streams;
    streams.reserve(stream_count);

    for (size_t index = 0; index < stream_count; ++index) {
      const DatabaseResult<std::vector<Database::StreamRangeEntry>> result =
          database_.XRead(stream_specs[index].first,
                          stream_specs[index].second);
      if (!result) {
        return tl::make_unexpected(
            MapDatabaseError(result.error(), "xread"));
      }
      if (!result->empty()) {
        streams.emplace_back(stream_specs[index].first, *result);
      }
    }

    if (streams.empty()) {
      return RespNullArray{};
    }
    return XreadResponse(streams);
  };

  if (!block_timeout.has_value()) {
    return read_streams();
  }

  const bool wait_forever =
      *block_timeout == std::chrono::steady_clock::duration::zero();
  const auto deadline = SaturatingDeadline(*block_timeout);
  while (true) {
    uint64_t observed_generation = 0;
    {
      std::lock_guard<std::recursive_mutex> execution_lock(
          transaction_mutex_);
      CommandResult result = read_streams();
      if (!result || !result->Is<RespNullArray>()) {
        return result;
      }
      observed_generation = database_.StreamGeneration();
    }

    std::chrono::steady_clock::duration remaining{};
    if (!wait_forever) {
      const auto now = std::chrono::steady_clock::now();
      if (now >= deadline) {
        return RespNullArray{};
      }
      remaining = deadline - now;
    }
    if (database_.WaitForStreamChange(observed_generation, remaining) ==
        WaitOutcome::kTimedOut) {
      return RespNullArray{};
    }
  }
}

CommandResult CommandExecutor::HandleRpush(
    const std::vector<std::string>& args) {
  if (args.size() < 3) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kWrongArity, .command = "rpush"});
  }

  const std::vector<std::string> values(args.begin() + 2, args.end());
  const DatabaseResult<int64_t> result =
      database_.PushRight(args[1], values);
  if (!result) {
    return tl::make_unexpected(MapDatabaseError(result.error(), "rpush"));
  }

  return RespInteger{*result};
}

CommandResult CommandExecutor::HandleLpush(
    const std::vector<std::string>& args) {
  if (args.size() < 3) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kWrongArity, .command = "lpush"});
  }

  const std::vector<std::string> values(args.begin() + 2, args.end());
  const DatabaseResult<int64_t> result =
      database_.PushLeft(args[1], values);
  if (!result) {
    return tl::make_unexpected(MapDatabaseError(result.error(), "lpush"));
  }

  return RespInteger{*result};
}

CommandResult CommandExecutor::HandleLrange(
    const std::vector<std::string>& args) {
  if (args.size() < 4) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kWrongArity, .command = "lrange"});
  }

  const std::optional<int64_t> start = ParseSignedInteger(args[2]);
  const std::optional<int64_t> stop = ParseSignedInteger(args[3]);
  if (!start.has_value() || !stop.has_value()) {
    return RespArray{};
  }

  const DatabaseResult<std::vector<std::string>> result =
      database_.Range(args[1], *start, *stop);
  if (!result) {
    return tl::make_unexpected(MapDatabaseError(result.error(), "lrange"));
  }

  return RespArray::BulkStrings(*result);
}

CommandResult CommandExecutor::HandleLlen(const std::vector<std::string>& args) {
  if (args.size() < 2) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "llen"});
  }

  const DatabaseResult<int64_t> result = database_.Length(args[1]);
  if (!result) {
    return tl::make_unexpected(MapDatabaseError(result.error(), "llen"));
  }

  return RespInteger{*result};
}

CommandResult CommandExecutor::HandleLpop(const std::vector<std::string>& args) {
  if (args.size() < 2) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "lpop"});
  }

  if (args.size() >= 3) {
    const std::optional<int64_t> count = ParseSignedInteger(args[2]);
    if (!count.has_value() || *count <= 0) {
      return RespArray{};
    }

    const DatabaseResult<std::vector<std::string>> result =
        database_.PopLeft(args[1], static_cast<size_t>(*count));
    if (!result) {
      return tl::make_unexpected(MapDatabaseError(result.error(), "lpop"));
    }

    return RespArray::BulkStrings(*result);
  }

  const DatabaseResult<std::optional<std::string>> result =
      database_.PopLeft(args[1]);
  if (!result) {
    return tl::make_unexpected(MapDatabaseError(result.error(), "lpop"));
  }
  if (!result->has_value()) {
    return RespNullBulk{};
  }

  return RespBulkString{**result};
}

CommandResult CommandExecutor::ExecuteBlockingPop(
    const std::vector<std::string>& args, CommandOrigin origin) {
  if (args.size() < 3) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kWrongArity, .command = "blpop"});
  }

  const std::optional<std::chrono::steady_clock::duration> timeout =
      ParseNonNegativeTimeout(args[2]);
  if (!timeout.has_value()) {
    return RespNullArray{};
  }

  const bool wait_forever =
      *timeout == std::chrono::steady_clock::duration::zero();
  const auto deadline = SaturatingDeadline(*timeout);
  while (true) {
    {
      std::lock_guard<std::recursive_mutex> execution_lock(
          transaction_mutex_);
      CommandResult result = HandleBlpopWithoutBlocking(args);
      if (!result || !result->Is<RespNullArray>()) {
        return FinishCommand(args, DescribeCommand(args[0]),
                             std::move(result), origin);
      }
    }

    std::chrono::steady_clock::duration remaining{};
    if (!wait_forever) {
      const auto now = std::chrono::steady_clock::now();
      if (now >= deadline) {
        return RespNullArray{};
      }
      remaining = deadline - now;
    }
    if (database_.WaitForListReady(args[1], remaining) ==
        WaitOutcome::kTimedOut) {
      return RespNullArray{};
    }
  }
}

CommandResult CommandExecutor::HandleBlpopWithoutBlocking(
    const std::vector<std::string>& args) {
  if (args.size() < 3) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kWrongArity, .command = "blpop"});
  }

  if (!ParseNonNegativeTimeout(args[2]).has_value()) {
    return RespNullArray{};
  }

  const DatabaseResult<std::optional<std::string>> result =
      database_.PopLeft(args[1]);
  if (!result) {
    return tl::make_unexpected(MapDatabaseError(result.error(), "blpop"));
  }
  if (!result->has_value()) {
    return RespNullArray{};
  }

  return RespArray::BulkStrings({args[1], **result});
}

}  // namespace redis
