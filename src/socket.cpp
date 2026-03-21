#include "redis-cpp/socket.hpp"

#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>

#include <string>

namespace redis {

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

ssize_t Socket::Receive(void* buffer, size_t size) const {
  return recv(fd_.Get(), buffer, size, 0);
}

Status Socket::SendAll(std::string_view data) const {
  size_t total_sent = 0;
  while (total_sent < data.size()) {
    const ssize_t sent =
        send(fd_.Get(), data.data() + total_sent, data.size() - total_sent, 0);
    if (sent <= 0) {
      return tl::make_unexpected(
          MakeNetworkError(NetworkErrorCode::kSendFailed));
    }
    total_sent += static_cast<size_t>(sent);
  }

  return {};
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
