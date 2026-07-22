#include "redis-cpp/client_session.hpp"

#include <optional>
#include <string_view>
#include <vector>

#include "redis-cpp/numeric_parser.hpp"
#include "redis-cpp/replica_manager.hpp"

namespace redis {
namespace {

constexpr size_t kReadBufferSize = 1024;

std::optional<int64_t> TryParseReplicaAck(
    const std::vector<std::string>& args) {
  if (args.size() != 3 || ToUpperAscii(args[0]) != "REPLCONF" ||
      ToUpperAscii(args[1]) != "ACK") {
    return std::nullopt;
  }
  return ParseNonNegativeInteger(args[2]);
}

std::string EncodeNoauthResponse() {
  return RespWriter::Error("NOAUTH Authentication required.");
}

}  // namespace

ClientSession::ClientSession(Socket socket, CommandExecutor& command_executor,
                             ReplicaManager* replica_manager,
                             PubSubManager* pubsub_manager)
    : socket_(std::move(socket)),
      writer_(std::make_shared<ConnectionWriter>(socket_.Get())),
      command_executor_(command_executor),
      replica_manager_(replica_manager),
      transaction_(command_executor),
      subscription_(writer_, pubsub_manager),
      authenticated_(command_executor.DefaultUserStartsAuthenticated()) {}

ClientSession::~ClientSession() {
  if (replica_registered_ && replica_manager_ != nullptr) {
    replica_manager_->RemoveReplica(socket_.Get());
  }
  writer_->Close();
}

Status ClientSession::SendResponse(const std::string& response) {
  return writer_->SendAll(response);
}

Status ClientSession::Run() {
  char buffer[kReadBufferSize];
  while (true) {
    Result<std::optional<size_t>> received =
        socket_.Receive(buffer, sizeof(buffer));
    if (!received) {
      return tl::make_unexpected(received.error());
    }
    if (!received->has_value()) {
      return {};
    }

    parser_.Append(std::string_view(buffer, **received));
    while (true) {
      Result<std::optional<std::vector<std::string>>> command =
          parser_.NextCommand();
      if (!command) {
        return tl::make_unexpected(command.error());
      }
      if (!command->has_value()) {
        break;
      }

      const std::vector<std::string>& args = **command;
      const std::string cmd = args.empty() ? "" : ToUpperAscii(args[0]);

      if (replica_manager_ != nullptr) {
        const std::optional<int64_t> replica_offset =
            TryParseReplicaAck(args);
        if (replica_offset.has_value() && replica_manager_->UpdateReplicaAck(
                                              socket_.Get(), *replica_offset)) {
          continue;
        }
      }

      if (!authenticated_ && cmd != "AUTH") {
        const std::string response = EncodeNoauthResponse();
        Status send_status = SendResponse(response);
        if (!send_status) {
          return send_status;
        }
        continue;
      }

      std::string response;
      std::optional<CommandResult> session_result;
      if (subscription_.IsSubscribed()) {
        session_result = subscription_.Process(args);
      }
      if (!session_result.has_value()) {
        session_result = transaction_.Process(args);
      }
      if (!session_result.has_value()) {
        session_result = subscription_.Process(args);
      }

      if (session_result.has_value()) {
        if (!*session_result) {
          response = RespWriter::Error(
              CommandErrorMessage(session_result->error()));
        } else {
          response = RespWriter::Write(**session_result);
        }
      } else if (cmd == "QUIT") {
        response = RespWriter::Write(RespSimpleString{"OK"});
        Status send_status = SendResponse(response);
        if (!send_status) {
          return send_status;
        }
        return {};
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
          Status send_status = SendResponse(response);
          if (!send_status) {
            return send_status;
          }
          Status add_status = replica_manager_->AddReplica(socket_.Get());
          if (!add_status) {
            return add_status;
          }
          replica_registered_ = true;
          // Stay in the receive loop; propagated commands arrive via
          // replica_manager_->PropagateToAll on other threads.
          continue;
        }
      }

      Status send_status = SendResponse(response);
      if (!send_status) {
        return send_status;
      }
    }
  }
}

}  // namespace redis
