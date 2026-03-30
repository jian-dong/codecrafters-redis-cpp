#include "redis-cpp/command_processor.hpp"

#include <chrono>
#include <cerrno>
#include <cstdint>
#include <optional>
#include <cstdlib>
#include <utility>
#include <vector>

#include "redis-cpp/resp.hpp"

namespace redis {
namespace {

constexpr std::string_view kMasterReplId =
    "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";

const std::string& GetEmptyRdb() {
  static const std::string rdb = []() {
    // Empty RDB file (Redis 7.2 format)
    static const uint8_t kBytes[] = {
        0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, 0xfa, 0x09,
        0x72, 0x65, 0x64, 0x69, 0x73, 0x2d, 0x76, 0x65, 0x72, 0x05, 0x37,
        0x2e, 0x32, 0x2e, 0x30, 0xfa, 0x0a, 0x72, 0x65, 0x64, 0x69, 0x73,
        0x2d, 0x62, 0x69, 0x74, 0x73, 0xc0, 0x40, 0xfa, 0x05, 0x63, 0x74,
        0x69, 0x6d, 0x65, 0xc2, 0x6d, 0x08, 0xbc, 0x65, 0xfa, 0x08, 0x75,
        0x73, 0x65, 0x64, 0x2d, 0x6d, 0x65, 0x6d, 0xc2, 0xb0, 0xc4, 0x10,
        0x00, 0xfa, 0x08, 0x61, 0x6f, 0x66, 0x2d, 0x62, 0x61, 0x73, 0x65,
        0xc0, 0x00, 0xff, 0xf0, 0x6e, 0x3b, 0xfe, 0xa0, 0xff, 0x5a, 0xa2};
    return std::string(reinterpret_cast<const char*>(kBytes), sizeof(kBytes));
  }();
  return rdb;
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

}  // namespace

CommandProcessor::CommandProcessor(Database& database, bool is_replica,
                                   ReplicaManager* replica_manager,
                                   const ServerConfig* server_config)
    : database_(database),
      is_replica_(is_replica),
      replica_manager_(replica_manager),
      server_config_(server_config) {}

std::string CommandErrorMessage(const CommandError& error) {
  switch (error.code) {
    case CommandErrorCode::kUnknownCommand:
      return "ERR unknown command '" + error.command + "'";
    case CommandErrorCode::kWrongArity:
      return "ERR wrong number of arguments for '" + error.command +
             "' command";
    case CommandErrorCode::kWrongType:
      return "WRONGTYPE Operation against a key holding the wrong kind of "
             "value";
    case CommandErrorCode::kSyntaxError:
      return "ERR syntax error";
    case CommandErrorCode::kInvalidInteger:
      return "ERR value is not an integer or out of range";
    case CommandErrorCode::kXaddIdNotGreaterThanZeroZero:
      return "ERR The ID specified in XADD must be greater than 0-0";
    case CommandErrorCode::kXaddIdNotGreaterThanTopItem:
      return "ERR The ID specified in XADD is equal or smaller than the "
             "target stream top item";
    case CommandErrorCode::kExecWithoutMulti:
      return "ERR EXEC without MULTI";
    case CommandErrorCode::kDiscardWithoutMulti:
      return "ERR DISCARD without MULTI";
  }

  return "ERR command failed";
}

CommandResult CommandProcessor::Execute(const std::vector<std::string>& args) {
  if (args.empty()) {
    return RespSimpleString{"PONG"};
  }

  const std::string command = ToUpperAscii(args[0]);
  if (command == "PING") {
    return HandlePing(args);
  }
  if (command == "ECHO") {
    return HandleEcho(args);
  }
  if (command == "SET") {
    return HandleSet(args);
  }
  if (command == "GET") {
    return HandleGet(args);
  }
  if (command == "KEYS") {
    return HandleKeys(args);
  }
  if (command == "SUBSCRIBE") {
    return HandleSubscribe(args);
  }
  if (command == "TYPE") {
    return HandleType(args);
  }
  if (command == "ZADD") {
    return HandleZadd(args);
  }
  if (command == "ZRANK") {
    return HandleZrank(args);
  }
  if (command == "XADD") {
    return HandleXadd(args);
  }
  if (command == "XRANGE") {
    return HandleXrange(args);
  }
  if (command == "XREAD") {
    return HandleXread(args);
  }
  if (command == "RPUSH") {
    return HandleRpush(args);
  }
  if (command == "LPUSH") {
    return HandleLpush(args);
  }
  if (command == "LRANGE") {
    return HandleLrange(args);
  }
  if (command == "LLEN") {
    return HandleLlen(args);
  }
  if (command == "LPOP") {
    return HandleLpop(args);
  }
  if (command == "BLPOP") {
    return HandleBlpop(args);
  }
  if (command == "INCR") {
    return HandleIncr(args);
  }
  if (command == "CONFIG") {
    return HandleConfig(args);
  }
  if (command == "INFO") {
    return HandleInfo(args);
  }
  if (command == "REPLCONF") {
    return HandleReplconf(args);
  }
  if (command == "WAIT") {
    return HandleWait(args);
  }
  if (command == "PSYNC") {
    const std::string& rdb = GetEmptyRdb();
    std::string response =
        "+FULLRESYNC " + std::string(kMasterReplId) + " 0\r\n";
    response += "$" + std::to_string(rdb.size()) + "\r\n";
    response += rdb;
    return RespRaw{std::move(response)};
  }
  if (command == "EXEC") {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kExecWithoutMulti,
                     .command = "exec"});
  }
  if (command == "DISCARD") {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kDiscardWithoutMulti,
                     .command = "discard"});
  }

  return tl::make_unexpected(CommandError{
      .code = CommandErrorCode::kUnknownCommand, .command = args[0]});
}

CommandResult CommandProcessor::HandlePing(
    const std::vector<std::string>& args) {
  if (args.size() >= 2) {
    return RespBulkString{args[1]};
  }

  return RespSimpleString{"PONG"};
}

CommandResult CommandProcessor::HandleEcho(
    const std::vector<std::string>& args) {
  if (args.size() < 2) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "echo"});
  }

  return RespBulkString{args[1]};
}

CommandResult CommandProcessor::HandleSet(
    const std::vector<std::string>& args) {
  if (args.size() < 3) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "set"});
  }

  std::optional<std::chrono::milliseconds> ttl;
  if (args.size() == 5) {
    if (ToUpperAscii(args[3]) != "PX") {
      return tl::make_unexpected(CommandError{
          .code = CommandErrorCode::kSyntaxError, .command = "set"});
    }

    int64_t ttl_milliseconds = 0;
    if (!ParseMilliseconds(args[4], ttl_milliseconds)) {
      return tl::make_unexpected(CommandError{
          .code = CommandErrorCode::kInvalidInteger, .command = "set"});
    }

    ttl = std::chrono::milliseconds(ttl_milliseconds);
  } else if (args.size() != 3) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kSyntaxError, .command = "set"});
  }

  database_.SetString(args[1], args[2], ttl);
  return RespSimpleString{"OK"};
}

CommandResult CommandProcessor::HandleGet(
    const std::vector<std::string>& args) {
  if (args.size() < 2) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "get"});
  }

  const Database::StringLookup result = database_.GetString(args[1]);
  if (result.type == ValueType::kNone) {
    return RespNullBulk{};
  }
  if (result.type != ValueType::kString) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongType, .command = "get"});
  }

  return RespBulkString{*result.value};
}

CommandResult CommandProcessor::HandleKeys(
    const std::vector<std::string>& args) {
  if (args.size() != 2) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "keys"});
  }

  if (args[1] != "*") {
    return RespArray{{}};
  }

  return RespArray{database_.Keys()};
}

CommandResult CommandProcessor::HandleSubscribe(
    const std::vector<std::string>& args) {
  if (args.size() != 2) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kWrongArity, .command = "subscribe"});
  }

  std::string response = "*3\r\n";
  response += "$9\r\nsubscribe\r\n";
  response += "$" + std::to_string(args[1].size()) + "\r\n" + args[1] + "\r\n";
  response += ":1\r\n";
  return RespRaw{std::move(response)};
}

CommandResult CommandProcessor::HandleType(
    const std::vector<std::string>& args) {
  if (args.size() < 2) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "type"});
  }

  return RespSimpleString{ValueTypeName(database_.TypeOf(args[1]))};
}

CommandResult CommandProcessor::HandleZadd(
    const std::vector<std::string>& args) {
  if (args.size() != 4) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "zadd"});
  }

  double score = 0.0;
  if (!ParseDouble(args[2], score)) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kSyntaxError, .command = "zadd"});
  }

  const Database::ZAddResult result = database_.ZAdd(args[1], score, args[3]);
  if (result.wrong_type) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongType, .command = "zadd"});
  }

  return RespInteger{result.added};
}

CommandResult CommandProcessor::HandleZrank(
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

CommandResult CommandProcessor::HandleXadd(
    const std::vector<std::string>& args) {
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

CommandResult CommandProcessor::HandleXrange(
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

CommandResult CommandProcessor::HandleXread(
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

CommandResult CommandProcessor::HandleRpush(
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

CommandResult CommandProcessor::HandleLpush(
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

CommandResult CommandProcessor::HandleLrange(
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

CommandResult CommandProcessor::HandleLlen(
    const std::vector<std::string>& args) {
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

CommandResult CommandProcessor::HandleLpop(
    const std::vector<std::string>& args) {
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

CommandResult CommandProcessor::HandleBlpop(
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

CommandResult CommandProcessor::HandleIncr(
    const std::vector<std::string>& args) {
  if (args.size() != 2) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "incr"});
  }

  const Database::IncrResult result = database_.Incr(args[1]);
  if (result.wrong_type) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongType, .command = "incr"});
  }
  if (result.not_integer) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kInvalidInteger, .command = "incr"});
  }

  return RespInteger{result.value};
}

CommandResult CommandProcessor::HandleInfo(
    const std::vector<std::string>& args) {
  const std::string section =
      args.size() >= 2 ? ToUpperAscii(args[1]) : "ALL";
  if (section == "REPLICATION" || section == "ALL") {
    std::string info = "# Replication\r\n";
    info += std::string("role:") + (is_replica_ ? "slave" : "master") + "\r\n";
    info += "connected_slaves:0\r\n";
    info += "master_replid:" + std::string(kMasterReplId) + "\r\n";
    info += "master_repl_offset:0\r\n";
    return RespBulkString{std::move(info)};
  }

  return RespBulkString{""};
}

CommandResult CommandProcessor::HandleConfig(
    const std::vector<std::string>& args) {
  if (args.size() != 3 || ToUpperAscii(args[1]) != "GET") {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity,
                     .command = "config"});
  }

  const std::string parameter = ToUpperAscii(args[2]);
  if (server_config_ == nullptr) {
    return RespArray{{}};
  }
  if (parameter == "DIR") {
    return RespArray{{"dir", server_config_->dir}};
  }
  if (parameter == "DBFILENAME") {
    return RespArray{{"dbfilename", server_config_->dbfilename}};
  }

  return RespArray{{}};
}

CommandResult CommandProcessor::HandleReplconf(
    const std::vector<std::string>& args) {
  if (is_replica_ && args.size() == 3 &&
      ToUpperAscii(args[1]) == "GETACK" && args[2] == "*") {
    return RespArray{{"REPLCONF", "ACK", "0"}};
  }

  return RespSimpleString{"OK"};
}

CommandResult CommandProcessor::HandleWait(
    const std::vector<std::string>& args) {
  if (args.size() != 3) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "wait"});
  }

  int64_t num_replicas = 0;
  int64_t timeout_milliseconds = 0;
  if (!ParseMilliseconds(args[1], num_replicas) ||
      !ParseMilliseconds(args[2], timeout_milliseconds)) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kInvalidInteger,
                     .command = "wait"});
  }

  if (replica_manager_ == nullptr) {
    return RespInteger{0};
  }

  return RespInteger{replica_manager_->WaitForReplicas(
      static_cast<size_t>(num_replicas),
      std::chrono::milliseconds(timeout_milliseconds))};
}

}  // namespace redis
