#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "redis-cpp/resp.hpp"

namespace redis {

enum class CommandId {
  kUnknown,
  kPing,
  kEcho,
  kSet,
  kGet,
  kWatch,
  kUnwatch,
  kMulti,
  kExec,
  kDiscard,
  kKeys,
  kAuth,
  kAcl,
  kPublish,
  kSubscribe,
  kPsubscribe,
  kUnsubscribe,
  kPunsubscribe,
  kReset,
  kQuit,
  kType,
  kGeoadd,
  kGeopos,
  kGeodist,
  kGeosearch,
  kZadd,
  kZrank,
  kZrange,
  kZcard,
  kZscore,
  kZrem,
  kXadd,
  kXrange,
  kXread,
  kRpush,
  kLpush,
  kLrange,
  kLlen,
  kLpop,
  kBlpop,
  kIncr,
  kConfig,
  kInfo,
  kReplconf,
  kWait,
  kPsync,
};

enum class CommandOrigin {
  kClient,
  kReplication,
  kAppendOnlyReplay,
  kTransaction,
};

struct CommandSemantics {
  CommandId id = CommandId::kUnknown;
  bool may_write = false;
  bool allowed_while_subscribed = false;
  bool serialized_with_transactions = true;
};

struct CommandEffects {
  std::optional<std::vector<std::string>> persistence;
  std::optional<std::vector<std::string>> replication;
};

CommandSemantics DescribeCommand(std::string_view name);
CommandEffects DescribeCommandEffects(
    const CommandSemantics& semantics,
    const std::vector<std::string>& submitted_command,
    const RespValue& reply, int64_t unix_time_milliseconds);

}  // namespace redis
