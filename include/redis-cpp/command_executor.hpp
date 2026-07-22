#pragma once

#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "redis-cpp/aof.hpp"
#include "redis-cpp/command.hpp"
#include "redis-cpp/database.hpp"
#include "redis-cpp/replica_manager.hpp"
#include "redis-cpp/resp.hpp"
#include "redis-cpp/server_config.hpp"
#include "tl/expected.hpp"

namespace redis {

class PubSubManager;
class Transaction;

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
  kWatchInsideMulti,
  kMultiInsideMulti,
  kPersistenceFailed,
};

struct CommandError {
  CommandErrorCode code = CommandErrorCode::kUnknownCommand;
  std::string command;
  std::string detail;
};

using CommandResult = tl::expected<RespValue, CommandError>;

struct TransactionExecution {
  bool aborted = false;
  std::optional<CommandError> error;
  std::vector<CommandResult> results;
};

std::string CommandErrorMessage(const CommandError& error);

class CommandExecutor {
 public:
  explicit CommandExecutor(Database& database, bool is_replica = false,
                           ReplicaManager* replica_manager = nullptr,
                           const ServerConfig* server_config = nullptr,
                           AppendOnlyLog* append_only_log = nullptr,
                           PubSubManager* pubsub_manager = nullptr);

  CommandResult Execute(
      const std::vector<std::string>& args,
      CommandOrigin origin = CommandOrigin::kClient);
  bool DefaultUserStartsAuthenticated() const;

 private:
  friend class Transaction;

  std::unordered_map<std::string, uint64_t> GetKeyVersions(
      const std::vector<std::string>& keys);
  TransactionExecution ExecuteTransaction(
      const std::vector<std::vector<std::string>>& commands,
      const std::unordered_map<std::string, uint64_t>& watched_key_versions,
      CommandOrigin origin);
  CommandResult ExecuteTransactionCommand(
      const std::vector<std::string>& command, CommandOrigin origin);
  CommandResult FinishCommand(
      const std::vector<std::string>& command,
      const CommandSemantics& semantics, CommandResult result,
      CommandOrigin origin);
  static CommandError MapDatabaseError(const DatabaseError& error,
                                       std::string command);
  CommandResult HandleBlpopWithoutBlocking(
      const std::vector<std::string>& args);
  CommandResult HandlePing(const std::vector<std::string>& args);
  CommandResult HandleEcho(const std::vector<std::string>& args);
  CommandResult HandleSet(const std::vector<std::string>& args);
  CommandResult HandleGet(const std::vector<std::string>& args);
  CommandResult HandleKeys(const std::vector<std::string>& args);
  CommandResult HandleAuth(const std::vector<std::string>& args);
  CommandResult HandleAcl(const std::vector<std::string>& args);
  CommandResult HandlePublish(const std::vector<std::string>& args);
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
  CommandResult ExecuteBlockingPop(const std::vector<std::string>& args,
                                   CommandOrigin origin);
  CommandResult HandleIncr(const std::vector<std::string>& args);
  CommandResult HandleConfig(const std::vector<std::string>& args);
  CommandResult HandleInfo(const std::vector<std::string>& args);
  CommandResult HandleReplconf(const std::vector<std::string>& args);
  CommandResult HandleWait(const std::vector<std::string>& args);
  bool DefaultUserAcceptsPassword(const std::string& password) const;
  RespValue DefaultUserDescription() const;
  void SetDefaultUserPassword(const std::string& password);
  bool DefaultUserUsesNoPassword() const;

  struct AclUserState {
    bool nopass = true;
    std::vector<std::string> password_hashes;
  };

  Database& database_;
  bool is_replica_ = false;
  ReplicaManager* replica_manager_ = nullptr;
  const ServerConfig* server_config_ = nullptr;
  AppendOnlyLog* append_only_log_ = nullptr;
  PubSubManager* pubsub_manager_ = nullptr;
  mutable std::mutex acl_mutex_;
  std::recursive_mutex transaction_mutex_;
  AclUserState default_user_;
};

}  // namespace redis
