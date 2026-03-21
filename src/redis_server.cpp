#include "redis-cpp/redis_server.hpp"

#include <iostream>
#include <string>
#include <thread>

#include "redis-cpp/client_session.hpp"

namespace redis {

RedisServer::RedisServer(ServerConfig config)
    : config_(config),
      command_processor_(database_, !config_.replicaof.empty()) {}

Status RedisServer::Run() {
  if (!config_.replicaof.empty()) {
    Status handshake = ConnectToMaster();
    if (!handshake) {
      return handshake;
    }
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

  char buf[256];

  // Step 1: PING
  Status status = master_socket_.SendAll("*1\r\n$4\r\nPING\r\n");
  if (!status) return status;
  master_socket_.Receive(buf, sizeof(buf));  // +PONG\r\n

  // Step 2: REPLCONF listening-port <PORT>
  const std::string port_str = std::to_string(config_.port);
  const std::string replconf_port =
      "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$" +
      std::to_string(port_str.size()) + "\r\n" + port_str + "\r\n";
  status = master_socket_.SendAll(replconf_port);
  if (!status) return status;
  master_socket_.Receive(buf, sizeof(buf));  // +OK\r\n

  // Step 3: REPLCONF capa psync2
  status = master_socket_.SendAll(
      "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n");
  if (!status) return status;
  master_socket_.Receive(buf, sizeof(buf));  // +OK\r\n

  // Step 4: PSYNC ? -1
  status = master_socket_.SendAll(
      "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n");
  if (!status) return status;
  master_socket_.Receive(buf, sizeof(buf));  // +FULLRESYNC <ID> 0\r\n

  return {};
}

void RedisServer::ServeClient(Socket socket) {
  std::cout << "Client connected\n";
  ClientSession session(std::move(socket), command_processor_, &replica_manager_);
  session.Run();
}

}  // namespace redis
