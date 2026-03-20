#include "redis-cpp/redis_server.hpp"

#include <iostream>
#include <thread>

#include "redis-cpp/client_session.hpp"

namespace redis {

RedisServer::RedisServer(ServerConfig config)
    : config_(config), command_processor_(database_) {}

Status RedisServer::Run() {
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

void RedisServer::ServeClient(Socket socket) {
  std::cout << "Client connected\n";
  ClientSession session(std::move(socket), command_processor_);
  session.Run();
}

}  // namespace redis
