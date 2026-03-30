#pragma once

#include <string>
#include <vector>

#include "redis-cpp/database.hpp"
#include "redis-cpp/replica_manager.hpp"
#include "redis-cpp/resp.hpp"
#include "redis-cpp/server_config.hpp"
#include "tl/expected.hpp"

namespace redis {

enum class CommandErrorCode {
  kUnknownCommand,
  kWrongArity,
  kWrongType,
  kSyntaxError,
  kInvalidInteger,
  kInvalidGeoCoordinates,
  kWrongPass,
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
  explicit CommandProcessor(Database& database, bool is_replica = false,
                            ReplicaManager* replica_manager = nullptr,
                            const ServerConfig* server_config = nullptr);

  CommandResult Execute(const std::vector<std::string>& args);

 private:
  CommandResult HandlePing(const std::vector<std::string>& args);
  CommandResult HandleEcho(const std::vector<std::string>& args);
  CommandResult HandleSet(const std::vector<std::string>& args);
  CommandResult HandleGet(const std::vector<std::string>& args);
  CommandResult HandleKeys(const std::vector<std::string>& args);
  CommandResult HandleAuth(const std::vector<std::string>& args);
  CommandResult HandleAcl(const std::vector<std::string>& args);
  CommandResult HandleSubscribe(const std::vector<std::string>& args);
  CommandResult HandleType(const std::vector<std::string>& args);
  CommandResult HandleGeoadd(const std::vector<std::string>& args);
  CommandResult HandleGeopos(const std::vector<std::string>& args);
  CommandResult HandleGeodist(const std::vector<std::string>& args);
  CommandResult HandleGeosearch(const std::vector<std::string>& args);
  CommandResult HandleZadd(const std::vector<std::string>& args);
  CommandResult HandleZrank(const std::vector<std::string>& args);
  CommandResult HandleZrange(const std::vector<std::string>& args);
  CommandResult HandleZcard(const std::vector<std::string>& args);
  CommandResult HandleZscore(const std::vector<std::string>& args);
  CommandResult HandleZrem(const std::vector<std::string>& args);
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
  CommandResult HandleConfig(const std::vector<std::string>& args);
  CommandResult HandleInfo(const std::vector<std::string>& args);
  CommandResult HandleReplconf(const std::vector<std::string>& args);
  CommandResult HandleWait(const std::vector<std::string>& args);

  struct AclUserState {
    bool nopass = true;
    std::vector<std::string> password_hashes;
  };

  Database& database_;
  bool is_replica_ = false;
  ReplicaManager* replica_manager_ = nullptr;
  const ServerConfig* server_config_ = nullptr;
  AclUserState default_user_;
};

}  // namespace redis
