#include "redis-cpp/command.hpp"

#include <array>
#include <cctype>
#include <limits>

#include "redis-cpp/numeric_parser.hpp"

namespace redis {
namespace {

struct CommandDefinition {
  std::string_view name;
  CommandSemantics semantics;
};

constexpr std::array kCommands{
    CommandDefinition{"PING", {CommandId::kPing, false, true}},
    CommandDefinition{"ECHO", {CommandId::kEcho}},
    CommandDefinition{"SET", {CommandId::kSet, true, false, true}},
    CommandDefinition{"GET", {CommandId::kGet}},
    CommandDefinition{"WATCH", {CommandId::kWatch}},
    CommandDefinition{"UNWATCH", {CommandId::kUnwatch}},
    CommandDefinition{"MULTI", {CommandId::kMulti}},
    CommandDefinition{"EXEC", {CommandId::kExec}},
    CommandDefinition{"DISCARD", {CommandId::kDiscard}},
    CommandDefinition{"KEYS", {CommandId::kKeys}},
    CommandDefinition{"AUTH", {CommandId::kAuth}},
    CommandDefinition{"ACL", {CommandId::kAcl}},
    CommandDefinition{"PUBLISH", {CommandId::kPublish}},
    CommandDefinition{"SUBSCRIBE", {CommandId::kSubscribe, false, true}},
    CommandDefinition{"PSUBSCRIBE", {CommandId::kPsubscribe, false, true}},
    CommandDefinition{"UNSUBSCRIBE", {CommandId::kUnsubscribe, false, true}},
    CommandDefinition{"PUNSUBSCRIBE", {CommandId::kPunsubscribe, false, true}},
    CommandDefinition{"RESET", {CommandId::kReset, false, true}},
    CommandDefinition{"QUIT", {CommandId::kQuit, false, true}},
    CommandDefinition{"TYPE", {CommandId::kType}},
    CommandDefinition{"GEOADD", {CommandId::kGeoadd, true, false, true}},
    CommandDefinition{"GEOPOS", {CommandId::kGeopos}},
    CommandDefinition{"GEODIST", {CommandId::kGeodist}},
    CommandDefinition{"GEOSEARCH", {CommandId::kGeosearch}},
    CommandDefinition{"ZADD", {CommandId::kZadd, true, false, true}},
    CommandDefinition{"ZRANK", {CommandId::kZrank}},
    CommandDefinition{"ZRANGE", {CommandId::kZrange}},
    CommandDefinition{"ZCARD", {CommandId::kZcard}},
    CommandDefinition{"ZSCORE", {CommandId::kZscore}},
    CommandDefinition{"ZREM", {CommandId::kZrem, true, false, true}},
    CommandDefinition{"XADD", {CommandId::kXadd, true, false, true}},
    CommandDefinition{"XRANGE", {CommandId::kXrange}},
    CommandDefinition{"XREAD", {CommandId::kXread}},
    CommandDefinition{"RPUSH", {CommandId::kRpush, true, false, true}},
    CommandDefinition{"LPUSH", {CommandId::kLpush, true, false, true}},
    CommandDefinition{"LRANGE", {CommandId::kLrange}},
    CommandDefinition{"LLEN", {CommandId::kLlen}},
    CommandDefinition{"LPOP", {CommandId::kLpop, true, false, true}},
    CommandDefinition{"BLPOP", {CommandId::kBlpop, true, false, false}},
    CommandDefinition{"INCR", {CommandId::kIncr, true, false, true}},
    CommandDefinition{"CONFIG", {CommandId::kConfig}},
    CommandDefinition{"INFO", {CommandId::kInfo}},
    CommandDefinition{"REPLCONF", {CommandId::kReplconf}},
    CommandDefinition{"WAIT", {CommandId::kWait}},
    CommandDefinition{"PSYNC", {CommandId::kPsync}},
};

bool EqualsIgnoreCase(std::string_view lhs, std::string_view rhs) {
  if (lhs.size() != rhs.size()) {
    return false;
  }
  for (size_t index = 0; index < lhs.size(); ++index) {
    const auto left = static_cast<unsigned char>(lhs[index]);
    const auto right = static_cast<unsigned char>(rhs[index]);
    if (std::toupper(left) != std::toupper(right)) {
      return false;
    }
  }
  return true;
}

bool IsEmptyMutationReply(CommandId id, const RespValue& reply) {
  if (id == CommandId::kZrem) {
    return reply.Is<RespInteger>() && reply.Get<RespInteger>().value == 0;
  }
  if (id == CommandId::kLpop) {
    return reply.Is<RespNullBulk>() ||
           (reply.Is<RespArray>() && reply.Get<RespArray>().values.empty());
  }
  if (id == CommandId::kBlpop) {
    return reply.Is<RespNullArray>();
  }
  return false;
}

std::vector<std::string> CanonicalCommand(
    CommandId id, const std::vector<std::string>& submitted_command,
    const RespValue& reply, int64_t unix_time_milliseconds) {
  std::vector<std::string> command = submitted_command;
  if (id == CommandId::kXadd && command.size() >= 3 &&
      reply.Is<RespBulkString>()) {
    command[2] = reply.Get<RespBulkString>().value;
  } else if (id == CommandId::kBlpop && command.size() >= 2) {
    command = {"LPOP", command[1]};
  } else if (id == CommandId::kSet && command.size() == 5 &&
             EqualsIgnoreCase(command[3], "PX")) {
    const std::optional<int64_t> ttl =
        ParseNonNegativeInteger(command[4]);
    if (ttl.has_value() &&
        unix_time_milliseconds <= std::numeric_limits<int64_t>::max() - *ttl) {
      command[3] = "PXAT";
      command[4] = std::to_string(unix_time_milliseconds + *ttl);
    }
  }
  return command;
}

}  // namespace

CommandSemantics DescribeCommand(std::string_view name) {
  for (const CommandDefinition& command : kCommands) {
    if (EqualsIgnoreCase(name, command.name)) {
      return command.semantics;
    }
  }
  return {};
}

CommandEffects DescribeCommandEffects(
    const CommandSemantics& semantics,
    const std::vector<std::string>& submitted_command,
    const RespValue& reply, int64_t unix_time_milliseconds) {
  if (semantics.id == CommandId::kPublish) {
    return {.persistence = std::nullopt,
            .replication = submitted_command};
  }
  if (!semantics.may_write || IsEmptyMutationReply(semantics.id, reply)) {
    return {};
  }

  std::vector<std::string> canonical = CanonicalCommand(
      semantics.id, submitted_command, reply, unix_time_milliseconds);
  return {
      .persistence = canonical,
      .replication = std::move(canonical),
  };
}

}  // namespace redis
