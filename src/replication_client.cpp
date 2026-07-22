#include "redis-cpp/replication_client.hpp"

#include <charconv>
#include <cstdint>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

#include "redis-cpp/resp.hpp"
#include "redis-cpp/transaction.hpp"

namespace redis {
namespace {

struct MasterEndpoint {
  std::string host;
  int port = 0;
};

Result<MasterEndpoint> ParseMasterEndpoint(std::string_view value) {
  const size_t separator = value.find_last_of(" \t");
  if (separator == std::string_view::npos) {
    return tl::make_unexpected(MakeReplicationError(
        ReplicationErrorCode::kInvalidMasterEndpoint));
  }

  std::string_view host = value.substr(0, separator);
  while (!host.empty() && (host.back() == ' ' || host.back() == '\t')) {
    host.remove_suffix(1);
  }
  std::string_view port_text = value.substr(separator + 1);
  while (!port_text.empty() &&
         (port_text.front() == ' ' || port_text.front() == '\t')) {
    port_text.remove_prefix(1);
  }

  int port = 0;
  const auto [next, error] = std::from_chars(
      port_text.data(), port_text.data() + port_text.size(), port);
  if (host.empty() || port_text.empty() || error != std::errc{} ||
      next != port_text.data() + port_text.size() || port < 1 ||
      port > 65535) {
    return tl::make_unexpected(MakeReplicationError(
        ReplicationErrorCode::kInvalidMasterEndpoint));
  }
  return MasterEndpoint{.host = std::string(host), .port = port};
}

class MasterReader {
 public:
  explicit MasterReader(Socket& socket) : socket_(socket) {}

  Result<std::string> ReadLine() {
    while (true) {
      const size_t line_end = buffer_.find("\r\n");
      if (line_end != std::string::npos) {
        std::string line = buffer_.substr(0, line_end);
        buffer_.erase(0, line_end + 2);
        return line;
      }
      Status fill_status = Fill();
      if (!fill_status) {
        return tl::make_unexpected(fill_status.error());
      }
    }
  }

  Status Skip(size_t size) {
    while (buffer_.size() < size) {
      Status fill_status = Fill();
      if (!fill_status) {
        return fill_status;
      }
    }
    buffer_.erase(0, size);
    return {};
  }

  std::string TakeBuffered() { return std::move(buffer_); }

 private:
  Status Fill() {
    char data[512];
    Result<std::optional<size_t>> received =
        socket_.Receive(data, sizeof(data));
    if (!received) {
      return tl::make_unexpected(received.error());
    }
    if (!received->has_value()) {
      return tl::make_unexpected(MakeReplicationError(
          ReplicationErrorCode::kMasterDisconnected));
    }
    buffer_.append(data, **received);
    return {};
  }

  Socket& socket_;
  std::string buffer_;
};

Status ExpectLine(Socket& socket, MasterReader& reader,
                  const std::vector<std::string>& command,
                  std::string_view expected,
                  bool allow_suffix = false) {
  Status send_status = socket.SendAll(RespWriter::WriteCommand(command));
  if (!send_status) {
    return send_status;
  }
  Result<std::string> response = reader.ReadLine();
  if (!response) {
    return tl::make_unexpected(response.error());
  }
  const bool matches = allow_suffix
                           ? std::string_view(*response).starts_with(expected)
                           : std::string_view(*response) == expected;
  if (!matches) {
    return tl::make_unexpected(MakeReplicationError(
        ReplicationErrorCode::kUnexpectedHandshake));
  }
  return {};
}

bool IsGetAck(const std::vector<std::string>& command) {
  return command.size() == 3 && ToUpperAscii(command[0]) == "REPLCONF" &&
         ToUpperAscii(command[1]) == "GETACK" && command[2] == "*";
}

}  // namespace

Status ReplicationClient::Connect(std::string master_endpoint,
                                  int listening_port) {
  Result<MasterEndpoint> endpoint = ParseMasterEndpoint(master_endpoint);
  if (!endpoint) {
    return tl::make_unexpected(endpoint.error());
  }

  Result<Socket> socket = Socket::Connect(endpoint->host, endpoint->port);
  if (!socket) {
    return tl::make_unexpected(socket.error());
  }
  master_socket_ = std::move(*socket);
  MasterReader reader(master_socket_);

  Status status = ExpectLine(master_socket_, reader, {"PING"}, "+PONG");
  if (!status) {
    return status;
  }
  status = ExpectLine(master_socket_, reader,
                      {"REPLCONF", "listening-port",
                       std::to_string(listening_port)},
                      "+OK");
  if (!status) {
    return status;
  }
  status = ExpectLine(master_socket_, reader,
                      {"REPLCONF", "capa", "psync2"}, "+OK");
  if (!status) {
    return status;
  }
  status = ExpectLine(master_socket_, reader, {"PSYNC", "?", "-1"},
                      "+FULLRESYNC ", true);
  if (!status) {
    return status;
  }

  Result<std::string> rdb_header = reader.ReadLine();
  if (!rdb_header || rdb_header->size() < 2 || rdb_header->front() != '$') {
    return tl::make_unexpected(MakeReplicationError(
        ReplicationErrorCode::kUnexpectedHandshake));
  }
  size_t rdb_size = 0;
  const char* begin = rdb_header->data() + 1;
  const char* end = rdb_header->data() + rdb_header->size();
  const auto [next, parse_error] = std::from_chars(begin, end, rdb_size);
  if (parse_error != std::errc{} || next != end) {
    return tl::make_unexpected(MakeReplicationError(
        ReplicationErrorCode::kUnexpectedHandshake));
  }
  status = reader.Skip(rdb_size);
  if (!status) {
    return status;
  }
  buffered_commands_ = reader.TakeBuffered();
  return {};
}

Status ReplicationClient::Run() {
  if (!master_socket_.IsValid()) {
    return tl::make_unexpected(MakeReplicationError(
        ReplicationErrorCode::kMasterDisconnected));
  }

  RespParser parser(RespParser::InputMode::kRespOnly);
  parser.Append(buffered_commands_);
  buffered_commands_.clear();
  Transaction transaction(executor_, CommandOrigin::kReplication);
  size_t replica_offset = 0;
  char buffer[1024];

  while (true) {
    while (parser.BufferSize() > 0) {
      const size_t size_before = parser.BufferSize();
      Result<std::optional<std::vector<std::string>>> command =
          parser.NextCommand();
      if (!command) {
        return tl::make_unexpected(command.error());
      }
      if (!command->has_value()) {
        break;
      }
      const size_t command_size = size_before - parser.BufferSize();

      if (IsGetAck(**command)) {
        Status send_status = master_socket_.SendAll(RespWriter::WriteCommand(
            {"REPLCONF", "ACK", std::to_string(replica_offset)}));
        if (!send_status) {
          return send_status;
        }
      } else {
        std::optional<CommandResult> transaction_result =
            transaction.Process(**command);
        if (transaction_result.has_value()) {
          if (!*transaction_result) {
            return tl::make_unexpected(MakeReplicationError(
                ReplicationErrorCode::kReplicatedCommandRejected));
          }
        } else {
          CommandResult result =
              executor_.Execute(**command, CommandOrigin::kReplication);
          if (!result) {
            return tl::make_unexpected(MakeReplicationError(
                ReplicationErrorCode::kReplicatedCommandRejected));
          }
        }
      }
      replica_offset += command_size;
    }

    Result<std::optional<size_t>> received =
        master_socket_.Receive(buffer, sizeof(buffer));
    if (!received) {
      return tl::make_unexpected(received.error());
    }
    if (!received->has_value()) {
      return tl::make_unexpected(MakeReplicationError(
          ReplicationErrorCode::kMasterDisconnected));
    }
    parser.Append(std::string_view(buffer, **received));
  }
}

}  // namespace redis
