#include "redis-cpp/resp.hpp"

#include <cctype>
#include <cerrno>
#include <charconv>
#include <climits>
#include <cstdlib>
#include <variant>

namespace redis {
namespace {

enum class ParseStatus {
  kOk,
  kIncomplete,
  kInvalid,
};

ParseStatus ParseNumber(std::string_view data, size_t& index, int& value) {
  value = 0;
  const size_t start = index;

  while (index < data.size() && data[index] >= '0' && data[index] <= '9') {
    const int digit = data[index] - '0';
    if (value > (INT_MAX - digit) / 10) {
      return ParseStatus::kInvalid;
    }

    value = value * 10 + digit;
    ++index;
  }

  if (start == index) {
    return ParseStatus::kInvalid;
  }

  if (index + 1 >= data.size()) {
    return ParseStatus::kIncomplete;
  }
  if (data[index] != '\r' || data[index + 1] != '\n') {
    return ParseStatus::kInvalid;
  }

  index += 2;
  return ParseStatus::kOk;
}

ParseStatus ParseBulkString(std::string_view data, size_t& index,
                            std::string& value) {
  if (index >= data.size()) {
    return ParseStatus::kIncomplete;
  }
  if (data[index] != '$') {
    return ParseStatus::kInvalid;
  }

  ++index;

  int bulk_length = 0;
  const ParseStatus number_status = ParseNumber(data, index, bulk_length);
  if (number_status != ParseStatus::kOk) {
    return number_status;
  }
  if (bulk_length < 0) {
    return ParseStatus::kInvalid;
  }

  if (index + static_cast<size_t>(bulk_length) + 2 > data.size()) {
    return ParseStatus::kIncomplete;
  }

  value.assign(data.data() + index, static_cast<size_t>(bulk_length));
  index += static_cast<size_t>(bulk_length);

  if (index + 1 >= data.size()) {
    return ParseStatus::kIncomplete;
  }
  if (data[index] != '\r' || data[index + 1] != '\n') {
    return ParseStatus::kInvalid;
  }

  index += 2;
  return ParseStatus::kOk;
}

ParseStatus ParseRespArray(std::string_view data, size_t& index,
                           std::vector<std::string>& args) {
  if (index >= data.size()) {
    return ParseStatus::kIncomplete;
  }
  if (data[index] != '*') {
    return ParseStatus::kInvalid;
  }

  ++index;

  int array_size = 0;
  const ParseStatus number_status = ParseNumber(data, index, array_size);
  if (number_status != ParseStatus::kOk) {
    return number_status;
  }
  if (array_size < 0) {
    return ParseStatus::kInvalid;
  }

  args.clear();
  args.reserve(static_cast<size_t>(array_size));
  for (int i = 0; i < array_size; ++i) {
    std::string value;
    const ParseStatus bulk_status = ParseBulkString(data, index, value);
    if (bulk_status != ParseStatus::kOk) {
      return bulk_status;
    }
    args.push_back(std::move(value));
  }

  return ParseStatus::kOk;
}

}  // namespace

void RespParser::Append(std::string_view data) { buffer_.append(data); }

Result<std::optional<std::vector<std::string>>> RespParser::NextCommand() {
  if (buffer_.empty()) {
    return std::optional<std::vector<std::string>>();
  }

  if (buffer_.front() == '*') {
    size_t index = 0;
    std::vector<std::string> args;
    const ParseStatus parse_status = ParseRespArray(buffer_, index, args);
    if (parse_status == ParseStatus::kIncomplete) {
      return std::optional<std::vector<std::string>>();
    }
    if (parse_status == ParseStatus::kInvalid) {
      return tl::make_unexpected(MakeRespError(RespErrorCode::kInvalidFrame));
    }

    buffer_.erase(0, index);
    return args;
  }

  const size_t newline = buffer_.find('\n');
  if (newline == std::string::npos) {
    return std::optional<std::vector<std::string>>();
  }

  buffer_.erase(0, newline + 1);
  return std::vector<std::string>{"PING"};
}

std::string RespWriter::Write(const RespValue& value) {
  return std::visit(
      [](const auto& response) -> std::string {
        using T = std::decay_t<decltype(response)>;
        if constexpr (std::is_same_v<T, RespSimpleString>) {
          return "+" + response.value + "\r\n";
        } else if constexpr (std::is_same_v<T, RespBulkString>) {
          return "$" + std::to_string(response.value.size()) + "\r\n" +
                 response.value + "\r\n";
        } else if constexpr (std::is_same_v<T, RespNullBulk>) {
          return "$-1\r\n";
        } else if constexpr (std::is_same_v<T, RespNullArray>) {
          return "*-1\r\n";
        } else if constexpr (std::is_same_v<T, RespInteger>) {
          return ":" + std::to_string(response.value) + "\r\n";
        } else if constexpr (std::is_same_v<T, RespRaw>) {
          return response.encoded;
        } else {
          std::string encoded =
              "*" + std::to_string(response.values.size()) + "\r\n";
          for (const std::string& item : response.values) {
            encoded +=
                "$" + std::to_string(item.size()) + "\r\n" + item + "\r\n";
          }
          return encoded;
        }
      },
      value);
}

std::string RespWriter::Error(std::string_view message) {
  return "-" + std::string(message) + "\r\n";
}

std::string ToUpperAscii(std::string value) {
  for (char& ch : value) {
    ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
  }
  return value;
}

bool ParseMilliseconds(std::string_view data, int64_t& value) {
  if (data.empty()) {
    return false;
  }

  const char* begin = data.data();
  const char* end = data.data() + data.size();
  const auto [ptr, error] = std::from_chars(begin, end, value);
  return error == std::errc{} && ptr == end && value >= 0;
}

bool ParseSignedInteger(std::string_view data, int64_t& value) {
  if (data.empty()) {
    return false;
  }

  const char* begin = data.data();
  const char* end = data.data() + data.size();
  const auto [ptr, error] = std::from_chars(begin, end, value);
  return error == std::errc{} && ptr == end;
}

bool ParseTimeoutDuration(std::string_view data,
                          std::chrono::steady_clock::duration& timeout) {
  if (data.empty()) {
    return false;
  }

  errno = 0;
  char* parse_end = nullptr;
  const std::string text(data);
  const double seconds = std::strtod(text.c_str(), &parse_end);
  if (parse_end != text.c_str() + text.size() || errno == ERANGE ||
      seconds < 0.0) {
    return false;
  }

  timeout = std::chrono::duration_cast<std::chrono::steady_clock::duration>(
      std::chrono::duration<double>(seconds));
  return true;
}

}  // namespace redis
