#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "redis-cpp/command_executor.hpp"

namespace redis {

class Transaction {
 public:
  explicit Transaction(CommandExecutor& executor,
                       CommandOrigin origin = CommandOrigin::kClient)
      : executor_(executor), origin_(origin) {}

  std::optional<CommandResult> Process(
      const std::vector<std::string>& command);

 private:
  CommandExecutor& executor_;
  CommandOrigin origin_;
  bool in_multi_ = false;
  std::vector<std::vector<std::string>> queued_commands_;
  std::unordered_map<std::string, uint64_t> watched_key_versions_;
};

}  // namespace redis
