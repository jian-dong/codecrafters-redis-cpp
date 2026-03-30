#include "redis-cpp/client_session.hpp"

#include <iostream>
#include <optional>
#include <sstream>
#include <string_view>
#include <vector>

#include "redis-cpp/replica_manager.hpp"

namespace redis {
namespace {

constexpr size_t kReadBufferSize = 1024;

void LogError(const Error& error) {
  if (error.ShouldLog()) {
    std::cerr << error.Message() << "\n";
  }
}

std::string EncodeAsRespArray(const std::vector<std::string>& args) {
  std::string encoded = "*" + std::to_string(args.size()) + "\r\n";
  for (const auto& arg : args) {
    encoded += "$" + std::to_string(arg.size()) + "\r\n" + arg + "\r\n";
  }
  return encoded;
}

bool IsWriteCommand(const std::string& cmd) {
  return cmd == "SET" || cmd == "RPUSH" || cmd == "LPUSH" || cmd == "LPOP" ||
         cmd == "XADD" || cmd == "INCR" || cmd == "GEOADD";
}

bool TryParseReplicaAck(const std::vector<std::string>& args, int64_t& offset) {
  return args.size() == 3 && ToUpperAscii(args[0]) == "REPLCONF" &&
         ToUpperAscii(args[1]) == "ACK" && ParseMilliseconds(args[2], offset);
}

size_t SubscriptionCount(const std::unordered_set<std::string>& channels,
                         const std::unordered_set<std::string>& patterns) {
  return channels.size() + patterns.size();
}

bool IsSubscribedModeAllowedCommand(const std::string& cmd) {
  return cmd == "SUBSCRIBE" || cmd == "UNSUBSCRIBE" || cmd == "PSUBSCRIBE" ||
         cmd == "PUNSUBSCRIBE" || cmd == "PING" || cmd == "QUIT" ||
         cmd == "RESET";
}

std::string EncodeSubscribeResponse(const std::string& channel,
                                    size_t subscribed_channel_count) {
  std::string response = "*3\r\n";
  response += "$9\r\nsubscribe\r\n";
  response += "$" + std::to_string(channel.size()) + "\r\n" + channel + "\r\n";
  response += ":" + std::to_string(subscribed_channel_count) + "\r\n";
  return response;
}

std::string EncodePsubscribeResponse(const std::string& pattern,
                                     size_t subscribed_count) {
  std::string response = "*3\r\n";
  response += "$10\r\npsubscribe\r\n";
  response += "$" + std::to_string(pattern.size()) + "\r\n" + pattern + "\r\n";
  response += ":" + std::to_string(subscribed_count) + "\r\n";
  return response;
}

std::string EncodeUnsubscribeFrame(std::string_view kind,
                                   const std::optional<std::string>& item,
                                   size_t subscribed_count) {
  std::string response = "*3\r\n";
  response += "$" + std::to_string(kind.size()) + "\r\n" + std::string(kind) +
              "\r\n";
  if (item.has_value()) {
    response += "$" + std::to_string(item->size()) + "\r\n" + *item + "\r\n";
  } else {
    response += "$-1\r\n";
  }
  response += ":" + std::to_string(subscribed_count) + "\r\n";
  return response;
}

std::string EncodeSubscribedModePingResponse() {
  return "*2\r\n$4\r\npong\r\n$0\r\n\r\n";
}

std::string EncodeNoauthResponse() {
  return RespWriter::Error("NOAUTH Authentication required.");
}

}  // namespace

ClientSession::ClientSession(Socket socket, CommandExecutor& command_executor,
                             ReplicaManager* replica_manager,
                             PubSubManager* pubsub_manager)
    : socket_(std::move(socket)),
      command_executor_(command_executor),
      replica_manager_(replica_manager),
      pubsub_manager_(pubsub_manager),
      authenticated_(command_executor.DefaultUserStartsAuthenticated()) {}

ClientSession::~ClientSession() {
  if (pubsub_manager_ == nullptr) {
    return;
  }

  for (const std::string& channel : subscribed_channels_) {
    pubsub_manager_->Unsubscribe(socket_.Get(), channel);
  }
}

bool ClientSession::SendResponse(const std::string& response) {
  Status send_status = socket_.SendAll(response);
  if (!send_status) {
    LogError(send_status.error());
    return false;
  }

  return true;
}

void ClientSession::Run() {
  char buffer[kReadBufferSize];
  while (true) {
    const ssize_t bytes_received = socket_.Receive(buffer, sizeof(buffer));
    if (bytes_received <= 0) {
      return;
    }

    parser_.Append(
        std::string_view(buffer, static_cast<size_t>(bytes_received)));
    while (true) {
      Result<std::optional<std::vector<std::string>>> command =
          parser_.NextCommand();
      if (!command) {
        LogError(command.error());
        return;
      }
      if (!command->has_value()) {
        break;
      }

      const std::vector<std::string>& args = **command;
      const std::string cmd = args.empty() ? "" : ToUpperAscii(args[0]);

      {
        std::ostringstream log;
        log << "[CMD]";
        for (const auto& a : args) log << " " << a;
        std::cerr << log.str() << "\n";
      }

      int64_t replica_offset = 0;
      if (replica_manager_ != nullptr &&
          TryParseReplicaAck(args, replica_offset) &&
          replica_manager_->UpdateReplicaAck(socket_.Get(), replica_offset)) {
        continue;
      }

      if (SubscriptionCount(subscribed_channels_, subscribed_patterns_) > 0 &&
          !IsSubscribedModeAllowedCommand(cmd)) {
        const std::string raw_command = args.empty() ? "" : ToUpperAscii(args[0]);
        const std::string response = RespWriter::Error(
            "ERR Can't execute '" + raw_command + "' in subscribed mode");
        if (!SendResponse(response)) {
          return;
        }
        continue;
      }

      if (!authenticated_ && cmd != "AUTH") {
        const std::string response = EncodeNoauthResponse();
        if (!SendResponse(response)) {
          return;
        }
        continue;
      }

      std::string response;
      if (cmd == "MULTI") {
        in_multi_ = true;
        response = "+OK\r\n";
      } else if (cmd == "PUBLISH" && args.size() == 3 && pubsub_manager_ != nullptr) {
        response =
            RespWriter::Write(RespInteger{pubsub_manager_->Publish(args[1], args[2])});
      } else if (cmd == "PING" &&
                 SubscriptionCount(subscribed_channels_, subscribed_patterns_) > 0) {
        response = EncodeSubscribedModePingResponse();
      } else if (cmd == "SUBSCRIBE" && args.size() == 2) {
        const auto [_, inserted] = subscribed_channels_.insert(args[1]);
        if (inserted && pubsub_manager_ != nullptr) {
          pubsub_manager_->Subscribe(socket_.Get(), args[1]);
        }
        response = EncodeSubscribeResponse(
            args[1],
            SubscriptionCount(subscribed_channels_, subscribed_patterns_));
      } else if (cmd == "PSUBSCRIBE" && args.size() == 2) {
        subscribed_patterns_.insert(args[1]);
        response = EncodePsubscribeResponse(
            args[1],
            SubscriptionCount(subscribed_channels_, subscribed_patterns_));
      } else if (cmd == "UNSUBSCRIBE" &&
                 SubscriptionCount(subscribed_channels_, subscribed_patterns_) > 0) {
        std::vector<std::optional<std::string>> targets;
        if (args.size() > 1) {
          targets.reserve(args.size() - 1);
          for (size_t index = 1; index < args.size(); ++index) {
            targets.push_back(args[index]);
          }
        } else if (!subscribed_channels_.empty()) {
          targets.reserve(subscribed_channels_.size());
          for (const std::string& channel : subscribed_channels_) {
            targets.push_back(channel);
          }
        } else {
          targets.push_back(std::nullopt);
        }

        for (const auto& target : targets) {
          if (target.has_value()) {
            const size_t erased = subscribed_channels_.erase(*target);
            if (erased > 0 && pubsub_manager_ != nullptr) {
              pubsub_manager_->Unsubscribe(socket_.Get(), *target);
            }
          }
          response += EncodeUnsubscribeFrame(
              "unsubscribe", target,
              SubscriptionCount(subscribed_channels_, subscribed_patterns_));
        }
      } else if (cmd == "PUNSUBSCRIBE" &&
                 SubscriptionCount(subscribed_channels_, subscribed_patterns_) > 0) {
        std::vector<std::optional<std::string>> targets;
        if (args.size() > 1) {
          targets.reserve(args.size() - 1);
          for (size_t index = 1; index < args.size(); ++index) {
            targets.push_back(args[index]);
          }
        } else if (!subscribed_patterns_.empty()) {
          targets.reserve(subscribed_patterns_.size());
          for (const std::string& pattern : subscribed_patterns_) {
            targets.push_back(pattern);
          }
        } else {
          targets.push_back(std::nullopt);
        }

        for (const auto& target : targets) {
          if (target.has_value()) {
            subscribed_patterns_.erase(*target);
          }
          response += EncodeUnsubscribeFrame(
              "punsubscribe", target,
              SubscriptionCount(subscribed_channels_, subscribed_patterns_));
        }
      } else if (cmd == "RESET") {
        if (pubsub_manager_ != nullptr) {
          for (const std::string& channel : subscribed_channels_) {
            pubsub_manager_->Unsubscribe(socket_.Get(), channel);
          }
        }
        subscribed_channels_.clear();
        subscribed_patterns_.clear();
        response = "+RESET\r\n";
      } else if (cmd == "QUIT") {
        response = "+OK\r\n";
        if (!SendResponse(response)) {
          return;
        }
        return;
      } else if (cmd == "EXEC" && in_multi_) {
        in_multi_ = false;
        response = "*" + std::to_string(queued_commands_.size()) + "\r\n";
        for (const std::vector<std::string>& queued_args : queued_commands_) {
          CommandResult command_result = command_executor_.Execute(queued_args);
          if (!command_result) {
            response += RespWriter::Error(CommandErrorMessage(command_result.error()));
          } else {
            response += RespWriter::Write(*command_result);
          }
        }
        queued_commands_.clear();
      } else if (cmd == "DISCARD" && in_multi_) {
        in_multi_ = false;
        queued_commands_.clear();
        response = "+OK\r\n";
      } else if (in_multi_) {
        queued_commands_.push_back(args);
        response = "+QUEUED\r\n";
      } else {
        CommandResult command_result = command_executor_.Execute(args);
        if (command_result && cmd == "AUTH") {
          authenticated_ = true;
        }
        if (!command_result) {
          response =
              RespWriter::Error(CommandErrorMessage(command_result.error()));
        } else {
          response = RespWriter::Write(*command_result);
        }

        if (command_result && cmd == "PSYNC" && replica_manager_ != nullptr) {
          if (!SendResponse(response)) {
            return;
          }
          replica_manager_->AddReplica(socket_.Get());
          // Stay in the receive loop; propagated commands arrive via
          // replica_manager_->PropagateToAll on other threads.
          continue;
        }

        if (command_result && IsWriteCommand(cmd) && replica_manager_ != nullptr) {
          replica_manager_->PropagateToAll(EncodeAsRespArray(args));
        }
      }

      if (!SendResponse(response)) {
        return;
      }
    }
  }
}

}  // namespace redis
