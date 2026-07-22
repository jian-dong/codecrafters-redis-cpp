#include "redis-cpp/transaction.hpp"

#include <utility>

#include "redis-cpp/command.hpp"

namespace redis {
namespace {

CommandResult WrongArity(std::string command) {
  return tl::make_unexpected(CommandError{
      .code = CommandErrorCode::kWrongArity, .command = std::move(command)});
}

CommandResult TransactionError(CommandErrorCode code, std::string command) {
  return tl::make_unexpected(
      CommandError{.code = code, .command = std::move(command)});
}

}  // namespace

std::optional<CommandResult> Transaction::Process(
    const std::vector<std::string>& command) {
  if (command.empty()) {
    return std::nullopt;
  }

  const CommandId id = DescribeCommand(command[0]).id;
  if (id == CommandId::kWatch) {
    if (in_multi_) {
      return TransactionError(CommandErrorCode::kWatchInsideMulti, "watch");
    }
    if (command.size() < 2) {
      return WrongArity("watch");
    }

    const std::vector<std::string> keys(command.begin() + 1, command.end());
    const auto versions = executor_.GetKeyVersions(keys);
    for (const auto& [key, version] : versions) {
      watched_key_versions_.try_emplace(key, version);
    }
    return CommandResult{RespSimpleString{"OK"}};
  }

  if (id == CommandId::kUnwatch) {
    if (command.size() != 1) {
      return WrongArity("unwatch");
    }
    watched_key_versions_.clear();
    return CommandResult{RespSimpleString{"OK"}};
  }

  if (id == CommandId::kMulti) {
    if (command.size() != 1) {
      return WrongArity("multi");
    }
    if (in_multi_) {
      return TransactionError(CommandErrorCode::kMultiInsideMulti, "multi");
    }
    in_multi_ = true;
    return CommandResult{RespSimpleString{"OK"}};
  }

  if (id == CommandId::kExec) {
    if (!in_multi_) {
      return TransactionError(CommandErrorCode::kExecWithoutMulti, "exec");
    }
    if (command.size() != 1) {
      return WrongArity("exec");
    }

    in_multi_ = false;
    TransactionExecution execution = executor_.ExecuteTransaction(
        queued_commands_, watched_key_versions_, origin_);
    queued_commands_.clear();
    watched_key_versions_.clear();
    if (execution.error.has_value()) {
      return tl::make_unexpected(*execution.error);
    }
    if (execution.aborted) {
      return CommandResult{RespNullArray{}};
    }

    std::vector<RespValue> responses;
    responses.reserve(execution.results.size());
    for (const CommandResult& result : execution.results) {
      if (!result) {
        responses.emplace_back(
            RespErrorReply{CommandErrorMessage(result.error())});
      } else {
        responses.push_back(*result);
      }
    }
    return CommandResult{RespArray{std::move(responses)}};
  }

  if (id == CommandId::kDiscard) {
    if (!in_multi_) {
      return TransactionError(CommandErrorCode::kDiscardWithoutMulti,
                              "discard");
    }
    if (command.size() != 1) {
      return WrongArity("discard");
    }
    in_multi_ = false;
    queued_commands_.clear();
    watched_key_versions_.clear();
    return CommandResult{RespSimpleString{"OK"}};
  }

  if (!in_multi_) {
    return std::nullopt;
  }

  queued_commands_.push_back(command);
  return CommandResult{RespSimpleString{"QUEUED"}};
}

}  // namespace redis
