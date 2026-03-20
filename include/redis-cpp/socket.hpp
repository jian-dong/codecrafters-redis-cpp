#ifndef REDIS_CPP_SOCKET_HPP_
#define REDIS_CPP_SOCKET_HPP_

#include <sys/types.h>

#include <optional>
#include <string_view>

#include "redis-cpp/result.hpp"
#include "redis-cpp/server_config.hpp"
#include "redis-cpp/unique_fd.hpp"

namespace redis {

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

  ssize_t Receive(void* buffer, size_t size) const;
  Status SendAll(std::string_view data) const;

 private:
  UniqueFd fd_;
};

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

#endif  // REDIS_CPP_SOCKET_HPP_
