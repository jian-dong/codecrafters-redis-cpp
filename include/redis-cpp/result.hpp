#ifndef REDIS_CPP_RESULT_HPP_
#define REDIS_CPP_RESULT_HPP_

#include <string>
#include <utility>
#include <variant>

#include "tl/expected.hpp"

namespace redis {

enum class CliErrorCode {
  kParseFailed,
};

struct CliError {
  CliErrorCode code = CliErrorCode::kParseFailed;
  int exit_code = 1;
};

enum class NetworkErrorCode {
  kSocketCreateFailed,
  kSetReuseAddressFailed,
  kBindFailed,
  kListenFailed,
  kAcceptFailed,
  kSendFailed,
};

struct NetworkError {
  NetworkErrorCode code = NetworkErrorCode::kSocketCreateFailed;
  int port = 0;
};

enum class RespErrorCode {
  kInvalidFrame,
};

struct RespError {
  RespErrorCode code = RespErrorCode::kInvalidFrame;
};

using ErrorKind = std::variant<CliError, NetworkError, RespError>;

class Error {
 public:
  explicit Error(CliError cli_error) : kind_(cli_error) {}
  explicit Error(NetworkError network_error) : kind_(network_error) {}
  explicit Error(RespError resp_error) : kind_(resp_error) {}

  [[nodiscard]] const ErrorKind& Kind() const { return kind_; }

  [[nodiscard]] int ExitCode() const {
    if (const auto* cli_error = std::get_if<CliError>(&kind_)) {
      return cli_error->exit_code;
    }
    return 1;
  }

  [[nodiscard]] bool ShouldLog() const {
    return !std::holds_alternative<CliError>(kind_);
  }

  [[nodiscard]] std::string Message() const {
    if (const auto* network_error = std::get_if<NetworkError>(&kind_)) {
      switch (network_error->code) {
        case NetworkErrorCode::kSocketCreateFailed:
          return "Failed to create server socket";
        case NetworkErrorCode::kSetReuseAddressFailed:
          return "setsockopt failed";
        case NetworkErrorCode::kBindFailed:
          return "Failed to bind to port " +
                 std::to_string(network_error->port);
        case NetworkErrorCode::kListenFailed:
          return "listen failed";
        case NetworkErrorCode::kAcceptFailed:
          return "Failed to accept client";
        case NetworkErrorCode::kSendFailed:
          return "Failed to send response";
      }
    }

    if (const auto* resp_error = std::get_if<RespError>(&kind_)) {
      switch (resp_error->code) {
        case RespErrorCode::kInvalidFrame:
          return "Failed to parse RESP array";
      }
    }

    return "";
  }

 private:
  ErrorKind kind_;
};

template <typename T>
using Result = tl::expected<T, Error>;

using Status = tl::expected<void, Error>;

inline Error MakeCliError(CliErrorCode code, int exit_code) {
  return Error(CliError{.code = code, .exit_code = exit_code});
}

inline Error MakeNetworkError(NetworkErrorCode code, int port = 0) {
  return Error(NetworkError{.code = code, .port = port});
}

inline Error MakeRespError(RespErrorCode code) {
  return Error(RespError{.code = code});
}

}  // namespace redis

#endif  // REDIS_CPP_RESULT_HPP_
