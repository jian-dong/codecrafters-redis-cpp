#pragma once

#include <cstddef>
#include <optional>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "redis-cpp/command_executor.hpp"
#include "redis-cpp/pubsub_manager.hpp"
#include "redis-cpp/socket.hpp"

namespace redis {

class SubscriptionSession {
 public:
  SubscriptionSession(SharedConnectionWriter writer = nullptr,
                      PubSubManager* pubsub_manager = nullptr)
      : writer_(std::move(writer)), pubsub_manager_(pubsub_manager) {}
  ~SubscriptionSession();

  SubscriptionSession(const SubscriptionSession&) = delete;
  SubscriptionSession& operator=(const SubscriptionSession&) = delete;

  [[nodiscard]] bool IsSubscribed() const;
  std::optional<CommandResult> Process(
      const std::vector<std::string>& command);

 private:
  [[nodiscard]] size_t SubscriptionCount() const;
  void Clear();

  SharedConnectionWriter writer_;
  PubSubManager* pubsub_manager_ = nullptr;
  std::unordered_set<std::string> channels_;
  std::unordered_set<std::string> patterns_;
};

}  // namespace redis
