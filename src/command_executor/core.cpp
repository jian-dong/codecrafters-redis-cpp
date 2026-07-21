#include "redis-cpp/command_executor.hpp"

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "redis-cpp/resp.hpp"

namespace redis {
namespace {

constexpr std::string_view kMasterReplId =
    "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";

bool IsMutatingCommand(const std::string& command) {
  return command == "SET" || command == "RPUSH" || command == "LPUSH" ||
         command == "LPOP" || command == "XADD" || command == "INCR" ||
         command == "GEOADD" || command == "ZADD" || command == "ZREM";
}

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

}  // namespace

CommandExecutor::CommandExecutor(Database& database, bool is_replica,
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
  }

  return "ERR command failed";
}

CommandResult CommandExecutor::Execute(const std::vector<std::string>& args) {
  if (args.empty()) {
    return RespSimpleString{"PONG"};
  }

  const std::string command = ToUpperAscii(args[0]);
  std::unique_lock<std::recursive_mutex> transaction_lock(transaction_mutex_,
                                                           std::defer_lock);
  if (IsMutatingCommand(command)) {
    transaction_lock.lock();
  }
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
  if (command == "WATCH") {
    return HandleWatch(args);
  }
  if (command == "UNWATCH") {
    return HandleUnwatch(args);
  }
  if (command == "MULTI") {
    return HandleMulti(args);
  }
  if (command == "KEYS") {
    return HandleKeys(args);
  }
  if (command == "AUTH") {
    return HandleAuth(args);
  }
  if (command == "ACL") {
    return HandleAcl(args);
  }
  if (command == "SUBSCRIBE") {
    return HandleSubscribe(args);
  }
  if (command == "TYPE") {
    return HandleType(args);
  }
  if (command == "GEOADD") {
    return HandleGeoadd(args);
  }
  if (command == "GEOPOS") {
    return HandleGeopos(args);
  }
  if (command == "GEODIST") {
    return HandleGeodist(args);
  }
  if (command == "GEOSEARCH") {
    return HandleGeosearch(args);
  }
  if (command == "ZADD") {
    return HandleZadd(args);
  }
  if (command == "ZRANK") {
    return HandleZrank(args);
  }
  if (command == "ZRANGE") {
    return HandleZrange(args);
  }
  if (command == "ZCARD") {
    return HandleZcard(args);
  }
  if (command == "ZSCORE") {
    return HandleZscore(args);
  }
  if (command == "ZREM") {
    return HandleZrem(args);
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
    const std::unordered_map<std::string, uint64_t>& watched_key_versions) {
  std::lock_guard<std::recursive_mutex> lock(transaction_mutex_);
  for (const auto& [key, watched_version] : watched_key_versions) {
    if (database_.KeyVersion(key) != watched_version) {
      return {.aborted = true};
    }
  }

  TransactionExecution execution;
  execution.results.reserve(commands.size());
  for (const std::vector<std::string>& command : commands) {
    execution.results.push_back(Execute(command));
  }
  return execution;
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

CommandResult CommandExecutor::HandleSet(const std::vector<std::string>& args) {
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

CommandResult CommandExecutor::HandleGet(const std::vector<std::string>& args) {
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

CommandResult CommandExecutor::HandleWatch(
    const std::vector<std::string>& args) {
  if (args.size() < 2) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kWrongArity, .command = "watch"});
  }

  return RespSimpleString{"OK"};
}

CommandResult CommandExecutor::HandleUnwatch(
    const std::vector<std::string>& args) {
  if (args.size() != 1) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kWrongArity, .command = "unwatch"});
  }

  return RespSimpleString{"OK"};
}

CommandResult CommandExecutor::HandleMulti(
    const std::vector<std::string>& args) {
  if (args.size() != 1) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kWrongArity, .command = "multi"});
  }

  return RespSimpleString{"OK"};
}

CommandResult CommandExecutor::HandleKeys(const std::vector<std::string>& args) {
  if (args.size() != 2) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "keys"});
  }

  if (args[1] != "*") {
    return RespArray{{}};
  }

  return RespArray{database_.Keys()};
}

CommandResult CommandExecutor::HandleSubscribe(
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

CommandResult CommandExecutor::HandleType(const std::vector<std::string>& args) {
  if (args.size() < 2) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "type"});
  }

  return RespSimpleString{ValueTypeName(database_.TypeOf(args[1]))};
}

CommandResult CommandExecutor::HandleIncr(const std::vector<std::string>& args) {
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

CommandResult CommandExecutor::HandleReplconf(
    const std::vector<std::string>& args) {
  if (is_replica_ && args.size() == 3 &&
      ToUpperAscii(args[1]) == "GETACK" && args[2] == "*") {
    return RespArray{{"REPLCONF", "ACK", "0"}};
  }

  return RespSimpleString{"OK"};
}

CommandResult CommandExecutor::HandleWait(const std::vector<std::string>& args) {
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
