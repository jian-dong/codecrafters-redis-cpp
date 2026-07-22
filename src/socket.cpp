#include "redis-cpp/socket.hpp"

#include <arpa/inet.h>
#include <cerrno>
#include <netdb.h>
#include <sys/socket.h>

#include <string>

namespace redis {
namespace {

void DisableSigpipe(int fd) {
#ifdef SO_NOSIGPIPE
  const int enabled = 1;
  (void)setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &enabled, sizeof(enabled));
#else
  (void)fd;
#endif
}

ssize_t SendWithoutSigpipe(int fd, const void* data, size_t size) {
#ifdef MSG_NOSIGNAL
  return send(fd, data, size, MSG_NOSIGNAL);
#else
  return send(fd, data, size, 0);
#endif
}

Status SendAllToFd(int fd, std::string_view data) {
  size_t total_sent = 0;
  while (total_sent < data.size()) {
    const ssize_t sent = SendWithoutSigpipe(
        fd, data.data() + total_sent, data.size() - total_sent);
    if (sent < 0 && errno == EINTR) {
      continue;
    }
    if (sent <= 0) {
      return tl::make_unexpected(
          MakeNetworkError(NetworkErrorCode::kSendFailed));
    }
    total_sent += static_cast<size_t>(sent);
  }
  return {};
}

}  // namespace

std::atomic<ConnectionId> ConnectionWriter::next_id_{1};

Result<Socket> Socket::Connect(const std::string& host, int port) {
  const std::string port_str = std::to_string(port);
  struct addrinfo hints{};
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;

  struct addrinfo* res = nullptr;
  if (getaddrinfo(host.c_str(), port_str.c_str(), &hints, &res) != 0 ||
      res == nullptr) {
    return tl::make_unexpected(
        MakeNetworkError(NetworkErrorCode::kConnectFailed, port));
  }

  UniqueFd fd(socket(res->ai_family, res->ai_socktype, res->ai_protocol));
  if (!fd.IsValid()) {
    freeaddrinfo(res);
    return tl::make_unexpected(
        MakeNetworkError(NetworkErrorCode::kConnectFailed, port));
  }

  const int connect_result = connect(fd.Get(), res->ai_addr, res->ai_addrlen);
  freeaddrinfo(res);
  if (connect_result != 0) {
    return tl::make_unexpected(
        MakeNetworkError(NetworkErrorCode::kConnectFailed, port));
  }

  return Socket(std::move(fd));
}

Result<std::optional<size_t>> Socket::Receive(void* buffer, size_t size) const {
  while (true) {
    const ssize_t received = recv(fd_.Get(), buffer, size, 0);
    if (received > 0) {
      return std::optional<size_t>(static_cast<size_t>(received));
    }
    if (received == 0) {
      return std::optional<size_t>();
    }
    if (errno != EINTR) {
      return tl::make_unexpected(
          MakeNetworkError(NetworkErrorCode::kReceiveFailed));
    }
  }
}

Status Socket::SendAll(std::string_view data) const {
  DisableSigpipe(fd_.Get());
  return SendAllToFd(fd_.Get(), data);
}

ConnectionWriter::ConnectionWriter(int fd)
    : id_(next_id_.fetch_add(1, std::memory_order_relaxed)),
      fd_(fd),
      open_(fd >= 0) {
  if (open_) {
    DisableSigpipe(fd_);
  }
}

bool ConnectionWriter::IsOpen() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return open_;
}

Status ConnectionWriter::SendAll(std::string_view data) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!open_) {
    return tl::make_unexpected(
        MakeNetworkError(NetworkErrorCode::kConnectionClosed));
  }
  Status result = SendAllToFd(fd_, data);
  if (!result) {
    open_ = false;
  }
  return result;
}

void ConnectionWriter::Close() {
  std::lock_guard<std::mutex> lock(mutex_);
  open_ = false;
}

Result<TcpListener> TcpListener::Open(const ServerConfig& config) {
  UniqueFd server_fd(socket(AF_INET, SOCK_STREAM, 0));
  if (!server_fd.IsValid()) {
    return tl::make_unexpected(
        MakeNetworkError(NetworkErrorCode::kSocketCreateFailed));
  }

  int reuse = 1;
  if (setsockopt(server_fd.Get(), SOL_SOCKET, SO_REUSEADDR, &reuse,
                 sizeof(reuse)) < 0) {
    return tl::make_unexpected(
        MakeNetworkError(NetworkErrorCode::kSetReuseAddressFailed));
  }

  sockaddr_in server_addr{};
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(static_cast<uint16_t>(config.port));

  if (bind(server_fd.Get(), reinterpret_cast<sockaddr*>(&server_addr),
           sizeof(server_addr)) != 0) {
    return tl::make_unexpected(
        MakeNetworkError(NetworkErrorCode::kBindFailed, config.port));
  }

  if (listen(server_fd.Get(), config.backlog) != 0) {
    return tl::make_unexpected(
        MakeNetworkError(NetworkErrorCode::kListenFailed));
  }

  return TcpListener(std::move(server_fd));
}

Result<Socket> TcpListener::Accept() const {
  sockaddr_in client_addr{};
  socklen_t client_addr_len = sizeof(client_addr);
  const int client_fd = accept(
      fd_.Get(), reinterpret_cast<sockaddr*>(&client_addr), &client_addr_len);
  if (client_fd < 0) {
    return tl::make_unexpected(
        MakeNetworkError(NetworkErrorCode::kAcceptFailed));
  }

  return Socket(UniqueFd(client_fd));
}

}  // namespace redis
