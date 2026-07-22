#include "redis-cpp/subscription_session.hpp"

#include <algorithm>
#include <string_view>
#include <utility>

#include "redis-cpp/command.hpp"
#include "redis-cpp/resp.hpp"

namespace redis {
namespace {

CommandResult WrongArity(std::string command) {
  return tl::make_unexpected(CommandError{
      .code = CommandErrorCode::kWrongArity, .command = std::move(command)});
}

RespValue SubscriptionFrame(std::string_view kind, const std::string& item,
                            size_t subscription_count) {
  return RespArray{std::vector<RespValue>{
      RespBulkString{std::string(kind)}, RespBulkString{item},
      RespInteger{static_cast<int64_t>(subscription_count)}}};
}

RespValue UnsubscriptionFrame(std::string_view kind,
                              const std::optional<std::string>& item,
                              size_t subscription_count) {
  std::vector<RespValue> values;
  values.emplace_back(RespBulkString{std::string(kind)});
  if (item.has_value()) {
    values.emplace_back(RespBulkString{*item});
  } else {
    values.emplace_back(RespNullBulk{});
  }
  values.emplace_back(
      RespInteger{static_cast<int64_t>(subscription_count)});
  return RespArray{std::move(values)};
}

CommandResult Frames(std::vector<RespValue> frames) {
  if (frames.size() == 1) {
    return std::move(frames.front());
  }
  return RespSequence{std::move(frames)};
}

std::vector<std::optional<std::string>> UnsubscriptionTargets(
    const std::vector<std::string>& command,
    const std::unordered_set<std::string>& subscriptions) {
  std::vector<std::optional<std::string>> targets;
  if (command.size() > 1) {
    targets.reserve(command.size() - 1);
    for (size_t index = 1; index < command.size(); ++index) {
      targets.emplace_back(command[index]);
    }
    return targets;
  }

  if (subscriptions.empty()) {
    targets.emplace_back(std::nullopt);
    return targets;
  }

  std::vector<std::string> sorted_subscriptions(subscriptions.begin(),
                                                 subscriptions.end());
  std::ranges::sort(sorted_subscriptions);
  targets.reserve(sorted_subscriptions.size());
  for (std::string& subscription : sorted_subscriptions) {
    targets.emplace_back(std::move(subscription));
  }
  return targets;
}

}  // namespace

SubscriptionSession::~SubscriptionSession() { Clear(); }

bool SubscriptionSession::IsSubscribed() const {
  return !channels_.empty() || !patterns_.empty();
}

size_t SubscriptionSession::SubscriptionCount() const {
  return channels_.size() + patterns_.size();
}

void SubscriptionSession::Clear() {
  if (pubsub_manager_ != nullptr) {
    for (const std::string& channel : channels_) {
      if (writer_ != nullptr) {
        pubsub_manager_->Unsubscribe(writer_->Id(), channel);
      }
    }
  }
  channels_.clear();
  patterns_.clear();
}

std::optional<CommandResult> SubscriptionSession::Process(
    const std::vector<std::string>& command) {
  if (command.empty()) {
    return std::nullopt;
  }

  const std::string normalized_name = ToUpperAscii(command[0]);
  const CommandSemantics semantics = DescribeCommand(normalized_name);
  if (IsSubscribed() && !semantics.allowed_while_subscribed) {
    return CommandResult{RespErrorReply{
        "ERR Can't execute '" + normalized_name + "' in subscribed mode"}};
  }

  switch (semantics.id) {
    case CommandId::kSubscribe: {
      if (command.size() < 2) {
        return WrongArity("subscribe");
      }

      std::vector<RespValue> frames;
      frames.reserve(command.size() - 1);
      for (size_t index = 1; index < command.size(); ++index) {
        const std::string& channel = command[index];
        const auto [_, inserted] = channels_.insert(channel);
        if (inserted && pubsub_manager_ != nullptr && writer_ != nullptr) {
          pubsub_manager_->Subscribe(writer_, channel);
        }
        frames.push_back(
            SubscriptionFrame("subscribe", channel, SubscriptionCount()));
      }
      return Frames(std::move(frames));
    }

    case CommandId::kPsubscribe: {
      if (command.size() < 2) {
        return WrongArity("psubscribe");
      }

      std::vector<RespValue> frames;
      frames.reserve(command.size() - 1);
      for (size_t index = 1; index < command.size(); ++index) {
        const std::string& pattern = command[index];
        patterns_.insert(pattern);
        frames.push_back(
            SubscriptionFrame("psubscribe", pattern, SubscriptionCount()));
      }
      return Frames(std::move(frames));
    }

    case CommandId::kUnsubscribe: {
      const auto targets = UnsubscriptionTargets(command, channels_);
      std::vector<RespValue> frames;
      frames.reserve(targets.size());
      for (const auto& target : targets) {
        if (target.has_value()) {
          const size_t erased = channels_.erase(*target);
          if (erased > 0 && pubsub_manager_ != nullptr && writer_ != nullptr) {
            pubsub_manager_->Unsubscribe(writer_->Id(), *target);
          }
        }
        frames.push_back(UnsubscriptionFrame("unsubscribe", target,
                                              SubscriptionCount()));
      }
      return Frames(std::move(frames));
    }

    case CommandId::kPunsubscribe: {
      const auto targets = UnsubscriptionTargets(command, patterns_);
      std::vector<RespValue> frames;
      frames.reserve(targets.size());
      for (const auto& target : targets) {
        if (target.has_value()) {
          patterns_.erase(*target);
        }
        frames.push_back(UnsubscriptionFrame("punsubscribe", target,
                                              SubscriptionCount()));
      }
      return Frames(std::move(frames));
    }

    case CommandId::kPing:
      if (!IsSubscribed()) {
        return std::nullopt;
      }
      if (command.size() > 2) {
        return WrongArity("ping");
      }
      return CommandResult{RespArray::BulkStrings(
          {"pong", command.size() == 2 ? command[1] : std::string{}})};

    case CommandId::kReset:
      if (command.size() != 1) {
        return WrongArity("reset");
      }
      Clear();
      return CommandResult{RespSimpleString{"RESET"}};

    default:
      return std::nullopt;
  }
}

}  // namespace redis
