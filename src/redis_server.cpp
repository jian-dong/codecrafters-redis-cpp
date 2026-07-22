#include "redis-cpp/redis_server.hpp"

#include <iostream>
#include <string>
#include <thread>

#include "redis-cpp/client_session.hpp"
#include "redis-cpp/rdb_loader.hpp"
#include "redis-cpp/resp.hpp"
#include "redis-cpp/transaction.hpp"

namespace redis {
namespace {

void LogError(const Error& error) {
  if (error.ShouldLog()) {
    std::cerr << error.Message() << "\n";
  }
}

}  // namespace

RedisServer::RedisServer(ServerConfig config)
    : config_(config),
      append_only_log_(MakeAppendOnlyLogConfig(config_)),
      command_executor_(database_, !config_.replicaof.empty(), &replica_manager_,
                        &config_, &append_only_log_, &pubsub_manager_),
      replication_client_(command_executor_) {}

Status RedisServer::Run() {
  Status open_status = append_only_log_.Open();
  if (!open_status) {
    return open_status;
  }

  Status rdb_status = LoadDatabaseFromRdb(config_, database_);
  if (!rdb_status) {
    return rdb_status;
  }

  Transaction replay_transaction(command_executor_,
                                 CommandOrigin::kAppendOnlyReplay);
  Status replay_status = append_only_log_.Replay(
      [this, &replay_transaction](
          const std::vector<std::string>& command) -> Status {
        std::optional<CommandResult> transaction_result =
            replay_transaction.Process(command);
        if (transaction_result.has_value()) {
          if (!*transaction_result) {
            return tl::make_unexpected(
                MakeRespError(RespErrorCode::kInvalidFrame));
          }
          return {};
        }
        CommandResult result = command_executor_.Execute(
            command, CommandOrigin::kAppendOnlyReplay);
        if (!result) {
          return tl::make_unexpected(
              MakeRespError(RespErrorCode::kInvalidFrame));
        }
        return {};
      });
  if (!replay_status) {
    return replay_status;
  }

  if (!config_.replicaof.empty()) {
    Status handshake =
        replication_client_.Connect(config_.replicaof, config_.port);
    if (!handshake) {
      return handshake;
    }
    std::thread([this] {
      Status replication_status = replication_client_.Run();
      if (!replication_status) {
        LogError(replication_status.error());
      }
    }).detach();
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

void RedisServer::ServeClient(Socket socket) {
  std::cout << "Client connected\n";
  ClientSession session(std::move(socket), command_executor_, &replica_manager_,
                        &pubsub_manager_);
  Status session_status = session.Run();
  if (!session_status) {
    LogError(session_status.error());
  }
}

}  // namespace redis
