#include "redis-cpp/command_executor.hpp"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "redis-cpp/command.hpp"
#include "redis-cpp/numeric_parser.hpp"
#include "redis-cpp/pubsub_manager.hpp"
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

int64_t UnixTimeMilliseconds() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

std::string EncodeTransaction(
    const std::vector<std::vector<std::string>>& commands) {
  std::string encoded = RespWriter::WriteCommand({"MULTI"});
  for (const std::vector<std::string>& command : commands) {
    encoded += RespWriter::WriteCommand(command);
  }
  encoded += RespWriter::WriteCommand({"EXEC"});
  return encoded;
}

}  // namespace

CommandExecutor::CommandExecutor(Database& database, bool is_replica,
                                 ReplicaManager* replica_manager,
                                 const ServerConfig* server_config,
                                 AppendOnlyLog* append_only_log,
                                 PubSubManager* pubsub_manager)
    : database_(database),
      is_replica_(is_replica),
      replica_manager_(replica_manager),
      server_config_(server_config),
      append_only_log_(append_only_log),
      pubsub_manager_(pubsub_manager) {}

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
    case CommandErrorCode::kInvalidGeoCoordinates:
      return "ERR invalid longitude,latitude pair";
    case CommandErrorCode::kWrongPass:
      return "WRONGPASS invalid username-password pair or user is disabled.";
    case CommandErrorCode::kXaddIdNotGreaterThanZeroZero:
      return "ERR The ID specified in XADD must be greater than 0-0";
    case CommandErrorCode::kXaddIdNotGreaterThanTopItem:
      return "ERR The ID specified in XADD is equal or smaller than the "
             "target stream top item";
    case CommandErrorCode::kExecWithoutMulti:
      return "ERR EXEC without MULTI";
    case CommandErrorCode::kDiscardWithoutMulti:
      return "ERR DISCARD without MULTI";
    case CommandErrorCode::kWatchInsideMulti:
      return "ERR WATCH inside MULTI is not allowed";
    case CommandErrorCode::kMultiInsideMulti:
      return "ERR MULTI calls can not be nested";
    case CommandErrorCode::kPersistenceFailed:
      return "MISCONF " + error.detail;
  }

  return "ERR command failed";
}

CommandError CommandExecutor::MapDatabaseError(const DatabaseError& error,
                                               std::string command) {
  CommandErrorCode command_code = CommandErrorCode::kSyntaxError;
  switch (error.code) {
    case DatabaseErrorCode::kWrongType:
      command_code = CommandErrorCode::kWrongType;
      break;
    case DatabaseErrorCode::kInvalidInteger:
    case DatabaseErrorCode::kIntegerOverflow:
      command_code = CommandErrorCode::kInvalidInteger;
      break;
    case DatabaseErrorCode::kInvalidStreamId:
      command_code = CommandErrorCode::kSyntaxError;
      break;
    case DatabaseErrorCode::kStreamIdNotGreaterThanZeroZero:
      command_code = CommandErrorCode::kXaddIdNotGreaterThanZeroZero;
      break;
    case DatabaseErrorCode::kStreamIdNotGreaterThanTopItem:
      command_code = CommandErrorCode::kXaddIdNotGreaterThanTopItem;
      break;
  }
  return CommandError{.code = command_code, .command = std::move(command)};
}

CommandResult CommandExecutor::Execute(const std::vector<std::string>& args,
                                       CommandOrigin origin) {
  if (args.empty()) {
    return RespSimpleString{"PONG"};
  }

  const CommandSemantics semantics = DescribeCommand(args[0]);
  if (semantics.id == CommandId::kBlpop &&
      origin != CommandOrigin::kAppendOnlyReplay &&
      origin != CommandOrigin::kTransaction) {
    return ExecuteBlockingPop(args, origin);
  }
  std::unique_lock<std::recursive_mutex> transaction_lock(transaction_mutex_,
                                                           std::defer_lock);
  const bool blocking_xread =
      semantics.id == CommandId::kXread && args.size() >= 2 &&
      ToUpperAscii(args[1]) == "BLOCK";
  if (semantics.serialized_with_transactions && !blocking_xread) {
    transaction_lock.lock();
  }
  CommandResult result = [&]() -> CommandResult {
    switch (semantics.id) {
      case CommandId::kPing:
        return HandlePing(args);
      case CommandId::kEcho:
        return HandleEcho(args);
      case CommandId::kSet:
        return HandleSet(args);
      case CommandId::kGet:
        return HandleGet(args);
      case CommandId::kKeys:
        return HandleKeys(args);
      case CommandId::kAuth:
        return HandleAuth(args);
      case CommandId::kAcl:
        return HandleAcl(args);
      case CommandId::kPublish:
        return HandlePublish(args);
      case CommandId::kSubscribe:
        return HandleSubscribe(args);
      case CommandId::kType:
        return HandleType(args);
      case CommandId::kGeoadd:
        return HandleGeoadd(args);
      case CommandId::kGeopos:
        return HandleGeopos(args);
      case CommandId::kGeodist:
        return HandleGeodist(args);
      case CommandId::kGeosearch:
        return HandleGeosearch(args);
      case CommandId::kZadd:
        return HandleZadd(args);
      case CommandId::kZrank:
        return HandleZrank(args);
      case CommandId::kZrange:
        return HandleZrange(args);
      case CommandId::kZcard:
        return HandleZcard(args);
      case CommandId::kZscore:
        return HandleZscore(args);
      case CommandId::kZrem:
        return HandleZrem(args);
      case CommandId::kXadd:
        return HandleXadd(args);
      case CommandId::kXrange:
        return HandleXrange(args);
      case CommandId::kXread:
        return HandleXread(args);
      case CommandId::kRpush:
        return HandleRpush(args);
      case CommandId::kLpush:
        return HandleLpush(args);
      case CommandId::kLrange:
        return HandleLrange(args);
      case CommandId::kLlen:
        return HandleLlen(args);
      case CommandId::kLpop:
        return HandleLpop(args);
      case CommandId::kBlpop:
        return HandleBlpopWithoutBlocking(args);
      case CommandId::kIncr:
        return HandleIncr(args);
      case CommandId::kConfig:
        return HandleConfig(args);
      case CommandId::kInfo:
        return HandleInfo(args);
      case CommandId::kReplconf:
        return HandleReplconf(args);
      case CommandId::kWait:
        return HandleWait(args);
      case CommandId::kPsync: {
        const std::string& rdb = GetEmptyRdb();
        return RespSequence{
            RespSimpleString{"FULLRESYNC " + std::string(kMasterReplId) +
                             " 0"},
            RespFileTransfer{rdb}};
      }
      case CommandId::kExec:
        return tl::make_unexpected(
            CommandError{.code = CommandErrorCode::kExecWithoutMulti,
                         .command = "exec"});
      case CommandId::kDiscard:
        return tl::make_unexpected(
            CommandError{.code = CommandErrorCode::kDiscardWithoutMulti,
                         .command = "discard"});
      default:
        return tl::make_unexpected(CommandError{
            .code = CommandErrorCode::kUnknownCommand, .command = args[0]});
    }
  }();

  return FinishCommand(args, semantics, std::move(result), origin);
}

CommandResult CommandExecutor::FinishCommand(
    const std::vector<std::string>& command,
    const CommandSemantics& semantics, CommandResult result,
    CommandOrigin origin) {
  if (!result || origin == CommandOrigin::kAppendOnlyReplay ||
      origin == CommandOrigin::kTransaction) {
    return result;
  }

  CommandEffects effects = DescribeCommandEffects(
      semantics, command, *result, UnixTimeMilliseconds());

  if (append_only_log_ != nullptr && effects.persistence.has_value()) {
    Status append_status = append_only_log_->Append(*effects.persistence);
    if (!append_status) {
      return tl::make_unexpected(CommandError{
          .code = CommandErrorCode::kPersistenceFailed,
          .command = command[0],
          .detail = append_status.error().Message()});
    }
  }
  if (replica_manager_ != nullptr && effects.replication.has_value()) {
    replica_manager_->PropagateToAll(
        RespWriter::WriteCommand(*effects.replication));
  }
  return result;
}

std::unordered_map<std::string, uint64_t> CommandExecutor::GetKeyVersions(
    const std::vector<std::string>& keys) {
  std::lock_guard<std::recursive_mutex> lock(transaction_mutex_);
  std::unordered_map<std::string, uint64_t> versions;
  versions.reserve(keys.size());
  for (const std::string& key : keys) {
    versions.try_emplace(key, database_.KeyVersion(key));
  }
  return versions;
}

TransactionExecution CommandExecutor::ExecuteTransaction(
    const std::vector<std::vector<std::string>>& commands,
    const std::unordered_map<std::string, uint64_t>& watched_key_versions,
    CommandOrigin origin) {
  std::lock_guard<std::recursive_mutex> lock(transaction_mutex_);
  for (const auto& [key, watched_version] : watched_key_versions) {
    if (database_.KeyVersion(key) != watched_version) {
      return {.aborted = true};
    }
  }

  TransactionExecution execution;
  execution.results.reserve(commands.size());
  std::vector<std::vector<std::string>> persistence_commands;
  std::vector<std::vector<std::string>> replication_commands;
  for (const std::vector<std::string>& command : commands) {
    CommandResult result = ExecuteTransactionCommand(command, origin);
    if (result) {
      const CommandEffects effects = DescribeCommandEffects(
          DescribeCommand(command.empty() ? "" : command[0]), command,
          *result, UnixTimeMilliseconds());
      if (effects.persistence.has_value()) {
        persistence_commands.push_back(*effects.persistence);
      }
      if (effects.replication.has_value()) {
        replication_commands.push_back(*effects.replication);
      }
    }
    execution.results.push_back(std::move(result));
  }

  if (origin == CommandOrigin::kAppendOnlyReplay) {
    return execution;
  }
  if (append_only_log_ != nullptr && !persistence_commands.empty()) {
    Status append_status =
        append_only_log_->AppendTransaction(persistence_commands);
    if (!append_status) {
      execution.error = CommandError{
          .code = CommandErrorCode::kPersistenceFailed,
          .command = "exec",
          .detail = append_status.error().Message(),
      };
      return execution;
    }
  }
  if (replica_manager_ != nullptr && !replication_commands.empty()) {
    replica_manager_->PropagateToAll(
        EncodeTransaction(replication_commands));
  }
  return execution;
}

CommandResult CommandExecutor::ExecuteTransactionCommand(
    const std::vector<std::string>& command, CommandOrigin origin) {
  const CommandId id =
      DescribeCommand(command.empty() ? "" : command[0]).id;
  if (id == CommandId::kBlpop) {
    return HandleBlpopWithoutBlocking(command);
  }

  if (id == CommandId::kXread && command.size() >= 3 &&
      ToUpperAscii(command[1]) == "BLOCK") {
    std::vector<std::string> non_blocking = command;
    non_blocking.erase(non_blocking.begin() + 1, non_blocking.begin() + 3);
    return Execute(non_blocking,
                   origin == CommandOrigin::kAppendOnlyReplay
                       ? CommandOrigin::kAppendOnlyReplay
                       : CommandOrigin::kTransaction);
  }

  return Execute(command, origin == CommandOrigin::kAppendOnlyReplay
                              ? CommandOrigin::kAppendOnlyReplay
                              : CommandOrigin::kTransaction);
}

CommandResult CommandExecutor::HandlePing(const std::vector<std::string>& args) {
  if (args.size() >= 2) {
    return RespBulkString{args[1]};
  }

  return RespSimpleString{"PONG"};
}

CommandResult CommandExecutor::HandleEcho(const std::vector<std::string>& args) {
  if (args.size() < 2) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "echo"});
  }

  return RespBulkString{args[1]};
}

CommandResult CommandExecutor::HandlePublish(
    const std::vector<std::string>& args) {
  if (args.size() != 3) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kWrongArity, .command = "publish"});
  }

  const int64_t subscriber_count =
      pubsub_manager_ == nullptr
          ? 0
          : pubsub_manager_->Publish(args[1], args[2]);
  return RespInteger{subscriber_count};
}

CommandResult CommandExecutor::HandleSet(const std::vector<std::string>& args) {
  if (args.size() < 3) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "set"});
  }

  std::optional<std::chrono::milliseconds> ttl;
  if (args.size() == 5) {
    const std::string expiry_mode = ToUpperAscii(args[3]);
    if (expiry_mode != "PX" && expiry_mode != "PXAT") {
      return tl::make_unexpected(CommandError{
          .code = CommandErrorCode::kSyntaxError, .command = "set"});
    }

    const std::optional<int64_t> ttl_milliseconds =
        ParseNonNegativeInteger(args[4]);
    if (!ttl_milliseconds.has_value()) {
      return tl::make_unexpected(CommandError{
          .code = CommandErrorCode::kInvalidInteger, .command = "set"});
    }

    if (expiry_mode == "PX") {
      ttl = std::chrono::milliseconds(*ttl_milliseconds);
    } else {
      const int64_t now_milliseconds =
          std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::system_clock::now().time_since_epoch())
              .count();
      ttl = std::chrono::milliseconds(
          std::max<int64_t>(0, *ttl_milliseconds - now_milliseconds));
    }
  } else if (args.size() != 3) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kSyntaxError, .command = "set"});
  }

  database_.SetString(args[1], args[2], ttl);
  return RespSimpleString{"OK"};
}

CommandResult CommandExecutor::HandleGet(const std::vector<std::string>& args) {
  if (args.size() < 2) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "get"});
  }

  const DatabaseResult<std::optional<std::string>> result =
      database_.GetString(args[1]);
  if (!result) {
    return tl::make_unexpected(MapDatabaseError(result.error(), "get"));
  }
  if (!result->has_value()) {
    return RespNullBulk{};
  }

  return RespBulkString{**result};
}

CommandResult CommandExecutor::HandleKeys(const std::vector<std::string>& args) {
  if (args.size() != 2) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "keys"});
  }

  if (args[1] != "*") {
    return RespArray{};
  }

  return RespArray::BulkStrings(database_.Keys());
}

CommandResult CommandExecutor::HandleSubscribe(
    const std::vector<std::string>& args) {
  if (args.size() != 2) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kWrongArity, .command = "subscribe"});
  }

  return RespArray{std::vector<RespValue>{RespBulkString{"subscribe"},
                                          RespBulkString{args[1]},
                                          RespInteger{1}}};
}

CommandResult CommandExecutor::HandleType(const std::vector<std::string>& args) {
  if (args.size() < 2) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "type"});
  }

  const std::optional<ValueType> type = database_.TypeOf(args[1]);
  return RespSimpleString{type.has_value() ? ValueTypeName(*type) : "none"};
}

CommandResult CommandExecutor::HandleIncr(const std::vector<std::string>& args) {
  if (args.size() != 2) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "incr"});
  }

  const DatabaseResult<int64_t> result = database_.Incr(args[1]);
  if (!result) {
    return tl::make_unexpected(MapDatabaseError(result.error(), "incr"));
  }

  return RespInteger{*result};
}

CommandResult CommandExecutor::HandleInfo(const std::vector<std::string>& args) {
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

CommandResult CommandExecutor::HandleConfig(
    const std::vector<std::string>& args) {
  if (args.size() != 3 || ToUpperAscii(args[1]) != "GET") {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity,
                     .command = "config"});
  }

  const std::string parameter = ToUpperAscii(args[2]);
  if (server_config_ == nullptr) {
    return RespArray{};
  }
  if (parameter == "DIR") {
    return RespArray::BulkStrings({"dir", server_config_->dir});
  }
  if (parameter == "DBFILENAME") {
    return RespArray::BulkStrings({"dbfilename", server_config_->dbfilename});
  }
  if (parameter == "APPENDONLY") {
    return RespArray::BulkStrings({"appendonly", server_config_->appendonly});
  }
  if (parameter == "APPENDDIRNAME") {
    return RespArray::BulkStrings(
        {"appenddirname", server_config_->appenddirname});
  }
  if (parameter == "APPENDFILENAME") {
    return RespArray::BulkStrings(
        {"appendfilename", server_config_->appendfilename});
  }
  if (parameter == "APPENDFSYNC") {
    return RespArray::BulkStrings({"appendfsync", server_config_->appendfsync});
  }

  return RespArray{};
}

CommandResult CommandExecutor::HandleReplconf(
    const std::vector<std::string>& args) {
  if (is_replica_ && args.size() == 3 &&
      ToUpperAscii(args[1]) == "GETACK" && args[2] == "*") {
    return RespArray::BulkStrings({"REPLCONF", "ACK", "0"});
  }

  return RespSimpleString{"OK"};
}

CommandResult CommandExecutor::HandleWait(const std::vector<std::string>& args) {
  if (args.size() != 3) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "wait"});
  }

  const std::optional<int64_t> num_replicas =
      ParseNonNegativeInteger(args[1]);
  const std::optional<int64_t> timeout_milliseconds =
      ParseNonNegativeInteger(args[2]);
  if (!num_replicas.has_value() || !timeout_milliseconds.has_value()) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kInvalidInteger,
                     .command = "wait"});
  }

  if (replica_manager_ == nullptr) {
    return RespInteger{0};
  }

  return RespInteger{replica_manager_->WaitForReplicas(
      static_cast<size_t>(*num_replicas),
      std::chrono::milliseconds(*timeout_milliseconds))};
}

}  // namespace redis
