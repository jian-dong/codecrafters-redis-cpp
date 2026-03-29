#pragma once

#include <string>
#include <vector>

#include "redis-cpp/database.hpp"
#include "redis-cpp/resp.hpp"
#include "tl/expected.hpp"

namespace redis {

enum class CommandErrorCode {
  kUnknownCommand,
  kWrongArity,
  kWrongType,
  kSyntaxError,
  kInvalidInteger,
  kXaddIdNotGreaterThanZeroZero,
  kXaddIdNotGreaterThanTopItem,
  kExecWithoutMulti,
  kDiscardWithoutMulti,
};

struct CommandError {
  CommandErrorCode code = CommandErrorCode::kUnknownCommand;
  std::string command;
};

using CommandResult = tl::expected<RespValue, CommandError>;

std::string CommandErrorMessage(const CommandError& error);

class CommandProcessor {
 public:
  explicit CommandProcessor(Database& database, bool is_replica = false);

  CommandResult Execute(const std::vector<std::string>& args);

 private:
  CommandResult HandlePing(const std::vector<std::string>& args);
  CommandResult HandleEcho(const std::vector<std::string>& args);
  CommandResult HandleSet(const std::vector<std::string>& args);
  CommandResult HandleGet(const std::vector<std::string>& args);
  CommandResult HandleType(const std::vector<std::string>& args);
  CommandResult HandleXadd(const std::vector<std::string>& args);
  CommandResult HandleXrange(const std::vector<std::string>& args);
  CommandResult HandleXread(const std::vector<std::string>& args);
  CommandResult HandleRpush(const std::vector<std::string>& args);
  CommandResult HandleLpush(const std::vector<std::string>& args);
  CommandResult HandleLrange(const std::vector<std::string>& args);
  CommandResult HandleLlen(const std::vector<std::string>& args);
  CommandResult HandleLpop(const std::vector<std::string>& args);
  CommandResult HandleBlpop(const std::vector<std::string>& args);
  CommandResult HandleIncr(const std::vector<std::string>& args);
  CommandResult HandleInfo(const std::vector<std::string>& args);
  CommandResult HandleReplconf(const std::vector<std::string>& args);
  CommandResult HandleWait(const std::vector<std::string>& args);

  Database& database_;
  bool is_replica_ = false;
};

}  // namespace redis
