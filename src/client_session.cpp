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
         cmd == "XADD" || cmd == "INCR";
}

bool TryParseReplicaAck(const std::vector<std::string>& args, int64_t& offset) {
  return args.size() == 3 && ToUpperAscii(args[0]) == "REPLCONF" &&
         ToUpperAscii(args[1]) == "ACK" && ParseMilliseconds(args[2], offset);
}

}  // namespace

ClientSession::ClientSession(Socket socket, CommandProcessor& command_processor,
                             ReplicaManager* replica_manager)
    : socket_(std::move(socket)),
      command_processor_(command_processor),
      replica_manager_(replica_manager) {}

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

      std::string response;
      if (cmd == "MULTI") {
        in_multi_ = true;
        response = "+OK\r\n";
      } else if (cmd == "EXEC" && in_multi_) {
        in_multi_ = false;
        response = "*" + std::to_string(queued_commands_.size()) + "\r\n";
        for (const std::vector<std::string>& queued_args : queued_commands_) {
          CommandResult command_result = command_processor_.Execute(queued_args);
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
        CommandResult command_result = command_processor_.Execute(args);
        if (!command_result) {
          response =
              RespWriter::Error(CommandErrorMessage(command_result.error()));
        } else {
          response = RespWriter::Write(*command_result);
        }

        if (command_result && cmd == "PSYNC" && replica_manager_ != nullptr) {
          Status send_status = socket_.SendAll(response);
          if (!send_status) {
            LogError(send_status.error());
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

      Status send_status = socket_.SendAll(response);
      if (!send_status) {
        LogError(send_status.error());
        return;
      }
    }
  }
}

}  // namespace redis
