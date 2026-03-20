#include "redis-cpp/client_session.hpp"

#include <iostream>
#include <optional>
#include <string_view>
#include <vector>

namespace redis {
namespace {

constexpr size_t kReadBufferSize = 1024;

void LogError(const Error& error) {
  if (error.ShouldLog()) {
    std::cerr << error.Message() << "\n";
  }
}

}  // namespace

ClientSession::ClientSession(Socket socket, CommandProcessor& command_processor)
    : socket_(std::move(socket)), command_processor_(command_processor) {}

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

      CommandResult command_result = command_processor_.Execute(**command);
      std::string response;
      if (!command_result) {
        response =
            RespWriter::Error(CommandErrorMessage(command_result.error()));
      } else {
        response = RespWriter::Write(*command_result);
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
