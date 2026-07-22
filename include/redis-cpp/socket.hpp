#pragma once

#include <sys/types.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>

#include "redis-cpp/result.hpp"
#include "redis-cpp/server_config.hpp"
#include "redis-cpp/unique_fd.hpp"

namespace redis {

using ConnectionId = uint64_t;

class Socket {
 public:
  Socket() = default;
  explicit Socket(UniqueFd fd) : fd_(std::move(fd)) {}

  Socket(const Socket&) = delete;
  Socket& operator=(const Socket&) = delete;
  Socket(Socket&&) noexcept = default;
  Socket& operator=(Socket&&) noexcept = default;

  [[nodiscard]] bool IsValid() const { return fd_.IsValid(); }
  [[nodiscard]] int Get() const { return fd_.Get(); }

  Result<std::optional<size_t>> Receive(void* buffer, size_t size) const;
  Status SendAll(std::string_view data) const;

  static Result<Socket> Connect(const std::string& host, int port);

 private:
  UniqueFd fd_;
};

class ConnectionWriter {
 public:
  explicit ConnectionWriter(int fd);

  ConnectionWriter(const ConnectionWriter&) = delete;
  ConnectionWriter& operator=(const ConnectionWriter&) = delete;

  [[nodiscard]] ConnectionId Id() const { return id_; }
  [[nodiscard]] bool IsOpen() const;
  Status SendAll(std::string_view data);
  void Close();

 private:
  static std::atomic<ConnectionId> next_id_;

  ConnectionId id_ = 0;
  int fd_ = -1;
  mutable std::mutex mutex_;
  bool open_ = false;
};

using SharedConnectionWriter = std::shared_ptr<ConnectionWriter>;

class TcpListener {
 public:
  static Result<TcpListener> Open(const ServerConfig& config);

  TcpListener() = default;
  explicit TcpListener(UniqueFd fd) : fd_(std::move(fd)) {}

  TcpListener(const TcpListener&) = delete;
  TcpListener& operator=(const TcpListener&) = delete;
  TcpListener(TcpListener&&) noexcept = default;
  TcpListener& operator=(TcpListener&&) noexcept = default;

  [[nodiscard]] bool IsValid() const { return fd_.IsValid(); }
  Result<Socket> Accept() const;

 private:
  UniqueFd fd_;
};

}  // namespace redis
