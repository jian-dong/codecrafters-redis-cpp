#include "redis-cpp/redis_server.hpp"

#include <iostream>
#include <string>
#include <thread>

#include "redis-cpp/client_session.hpp"
#include "redis-cpp/resp.hpp"

namespace redis {
namespace {

bool IsReplconfGetack(const std::vector<std::string>& args) {
  return args.size() == 3 && ToUpperAscii(args[0]) == "REPLCONF" &&
         ToUpperAscii(args[1]) == "GETACK" && args[2] == "*";
}

// Buffered reader for the master socket during handshake.
class MasterReader {
 public:
  explicit MasterReader(Socket& sock) : sock_(sock) {}

  // Read a CRLF-terminated line; returns it without the trailing \r\n.
  std::string ReadLine() {
    while (true) {
      const auto pos = buf_.find("\r\n");
      if (pos != std::string::npos) {
        std::string line = buf_.substr(0, pos);
        buf_ = buf_.substr(pos + 2);
        return line;
      }
      if (!Fill()) return "";
    }
  }

  // Read and discard exactly n bytes.
  void Skip(size_t n) {
    while (buf_.size() < n) {
      if (!Fill()) return;
    }
    buf_ = buf_.substr(n);
  }

  // Take any bytes already buffered past what we've consumed.
  std::string TakeLeftover() { return std::move(buf_); }

 private:
  bool Fill() {
    char tmp[512];
    const ssize_t n = sock_.Receive(tmp, sizeof(tmp));
    if (n <= 0) return false;
    buf_.append(tmp, static_cast<size_t>(n));
    return true;
  }

  Socket& sock_;
  std::string buf_;
};

}  // namespace

RedisServer::RedisServer(ServerConfig config)
    : config_(config),
      command_processor_(database_, !config_.replicaof.empty(),
                         &replica_manager_) {}

Status RedisServer::Run() {
  if (!config_.replicaof.empty()) {
    Status handshake = ConnectToMaster();
    if (!handshake) {
      return handshake;
    }
    std::thread(&RedisServer::ProcessReplicatedCommands, this).detach();
  }

  Result<TcpListener> listener = TcpListener::Open(config_);
  if (!listener) {
    return tl::make_unexpected(listener.error());
  }

  std::cout << "Waiting for a client to connect...\n";
  std::cout << "Logs from your program will appear here!\n";

  while (true) {
    Result<Socket> socket = listener->Accept();
    if (!socket) {
      if (socket.error().ShouldLog()) {
        std::cerr << socket.error().Message() << "\n";
      }
      continue;
    }

    std::thread(&RedisServer::ServeClient, this, std::move(*socket)).detach();
  }
}

Status RedisServer::ConnectToMaster() {
  const size_t space = config_.replicaof.find(' ');
  const std::string master_host = config_.replicaof.substr(0, space);
  const int master_port = std::stoi(config_.replicaof.substr(space + 1));

  Result<Socket> socket = Socket::Connect(master_host, master_port);
  if (!socket) {
    return tl::make_unexpected(socket.error());
  }
  master_socket_ = std::move(*socket);

  MasterReader reader(master_socket_);

  // Step 1: PING
  Status status = master_socket_.SendAll("*1\r\n$4\r\nPING\r\n");
  if (!status) return status;
  reader.ReadLine();  // +PONG

  // Step 2: REPLCONF listening-port <PORT>
  const std::string port_str = std::to_string(config_.port);
  const std::string replconf_port =
      "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$" +
      std::to_string(port_str.size()) + "\r\n" + port_str + "\r\n";
  status = master_socket_.SendAll(replconf_port);
  if (!status) return status;
  reader.ReadLine();  // +OK

  // Step 3: REPLCONF capa psync2
  status = master_socket_.SendAll(
      "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n");
  if (!status) return status;
  reader.ReadLine();  // +OK

  // Step 4: PSYNC ? -1 → +FULLRESYNC <ID> <offset>\r\n$N\r\n<N bytes rdb>
  status = master_socket_.SendAll(
      "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n");
  if (!status) return status;
  reader.ReadLine();  // +FULLRESYNC ...

  // Read and discard RDB: $N\r\n<N bytes> (no trailing \r\n)
  const std::string rdb_header = reader.ReadLine();  // $88
  if (!rdb_header.empty() && rdb_header[0] == '$') {
    const size_t rdb_size = std::stoul(rdb_header.substr(1));
    reader.Skip(rdb_size);
  }

  // Any bytes already received beyond the RDB are the start of propagated cmds.
  master_leftover_ = reader.TakeLeftover();

  return {};
}

void RedisServer::ProcessReplicatedCommands() {
  RespParser parser;
  size_t replica_offset = 0;

  // Feed any bytes received during handshake that came after the RDB.
  if (!master_leftover_.empty()) {
    parser.Append(master_leftover_);
    master_leftover_.clear();
  }

  char buffer[1024];
  while (true) {
    // Drain all fully-buffered commands before blocking on recv.
    while (true) {
      const size_t buf_before = parser.BufferSize();
      Result<std::optional<std::vector<std::string>>> cmd =
          parser.NextCommand();
      if (!cmd || !cmd->has_value()) break;
      const size_t cmd_bytes = buf_before - parser.BufferSize();

      if (IsReplconfGetack(**cmd)) {
        const std::string response = RespWriter::Write(
            RespArray{{"REPLCONF", "ACK", std::to_string(replica_offset)}});
        Status send_status = master_socket_.SendAll(response);
        if (!send_status) return;
        replica_offset += cmd_bytes;
        continue;
      }

      (void)command_processor_.Execute(**cmd);
      replica_offset += cmd_bytes;
    }

    const ssize_t n = master_socket_.Receive(buffer, sizeof(buffer));
    if (n <= 0) return;
    parser.Append(std::string_view(buffer, static_cast<size_t>(n)));
  }
}

void RedisServer::ServeClient(Socket socket) {
  std::cout << "Client connected\n";
  ClientSession session(std::move(socket), command_processor_, &replica_manager_);
  session.Run();
}

}  // namespace redis
