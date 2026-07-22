#include "redis-cpp/resp.hpp"

#include <cctype>
#include <climits>
#include <sstream>
#include <variant>

namespace redis {
namespace {

enum class ParseStatus {
  kOk,
  kIncomplete,
  kInvalid,
  kTooLarge,
};

// Keep a single client command bounded even when its declared RESP lengths are
// malicious. AOF and replication streams may contain arbitrarily many
// commands; the budget applies to each self-framed command, not the whole
// buffered stream.
constexpr size_t kMaxCommandBytes = 16U * 1024U * 1024U;
constexpr int kMaxCommandArguments = 16 * 1024;
constexpr int kMaxBulkStringBytes = 16 * 1024 * 1024;

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
  if (bulk_length > kMaxBulkStringBytes) {
    return ParseStatus::kTooLarge;
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
  if (array_size <= 0) {
    return ParseStatus::kInvalid;
  }
  if (array_size > kMaxCommandArguments) {
    return ParseStatus::kTooLarge;
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

namespace {

std::string SafeRespLine(std::string value) {
  for (char& character : value) {
    if (character == '\r' || character == '\n') {
      character = ' ';
    }
  }
  return value;
}

}  // namespace

RespArray::RespArray(std::vector<RespValue> values)
    : values(std::move(values)) {}

RespArray RespArray::BulkStrings(std::vector<std::string> bulk_strings) {
  std::vector<RespValue> values;
  values.reserve(bulk_strings.size());
  for (std::string& item : bulk_strings) {
    values.emplace_back(RespBulkString{std::move(item)});
  }
  return RespArray{std::move(values)};
}

RespSequence::RespSequence(std::vector<RespValue> values)
    : values(std::move(values)) {}

RespSequence::RespSequence(std::initializer_list<RespValue> values)
    : values(values) {}

void RespParser::Append(std::string_view data) {
  if (consumed_ == buffer_.size()) {
    buffer_.clear();
    consumed_ = 0;
  } else if (consumed_ > 0) {
    buffer_.erase(0, consumed_);
    consumed_ = 0;
  }
  buffer_.append(data);
}

Result<std::optional<std::vector<std::string>>> RespParser::NextCommand() {
  const std::string_view input(buffer_.data() + consumed_, BufferSize());
  if (input.empty()) {
    return std::optional<std::vector<std::string>>();
  }

  if (input.front() == '*') {
    size_t index = 0;
    std::vector<std::string> args;
    const ParseStatus parse_status = ParseRespArray(input, index, args);
    if (parse_status == ParseStatus::kIncomplete) {
      if (input.size() > kMaxCommandBytes) {
        return tl::make_unexpected(
            MakeRespError(RespErrorCode::kFrameTooLarge));
      }
      return std::optional<std::vector<std::string>>();
    }
    if (parse_status == ParseStatus::kTooLarge ||
        index > kMaxCommandBytes) {
      return tl::make_unexpected(
          MakeRespError(RespErrorCode::kFrameTooLarge));
    }
    if (parse_status == ParseStatus::kInvalid) {
      return tl::make_unexpected(MakeRespError(RespErrorCode::kInvalidFrame));
    }

    consumed_ += index;
    return args;
  }

  if (input_mode_ == InputMode::kRespOnly) {
    return tl::make_unexpected(MakeRespError(RespErrorCode::kInvalidFrame));
  }

  const size_t newline = input.find('\n');
  if (newline == std::string::npos) {
    if (input.size() > kMaxCommandBytes) {
      return tl::make_unexpected(
          MakeRespError(RespErrorCode::kFrameTooLarge));
    }
    return std::optional<std::vector<std::string>>();
  }
  if (newline + 1 > kMaxCommandBytes) {
    return tl::make_unexpected(
        MakeRespError(RespErrorCode::kFrameTooLarge));
  }

  std::string line(input.substr(0, newline));
  consumed_ += newline + 1;
  if (!line.empty() && line.back() == '\r') {
    line.pop_back();
  }

  std::istringstream fields(line);
  std::vector<std::string> args;
  for (std::string field; fields >> field;) {
    args.push_back(std::move(field));
  }
  if (args.empty()) {
    return tl::make_unexpected(MakeRespError(RespErrorCode::kInvalidFrame));
  }
  return args;
}

std::string RespWriter::Write(const RespValue& value) {
  return value.Visit(
      [](const auto& response) -> std::string {
        using T = std::decay_t<decltype(response)>;
        if constexpr (std::is_same_v<T, RespSimpleString>) {
          return "+" + SafeRespLine(response.value) + "\r\n";
        } else if constexpr (std::is_same_v<T, RespBulkString>) {
          return "$" + std::to_string(response.value.size()) + "\r\n" +
                 response.value + "\r\n";
        } else if constexpr (std::is_same_v<T, RespErrorReply>) {
          return "-" + SafeRespLine(response.value) + "\r\n";
        } else if constexpr (std::is_same_v<T, RespNullBulk>) {
          return "$-1\r\n";
        } else if constexpr (std::is_same_v<T, RespNullArray>) {
          return "*-1\r\n";
        } else if constexpr (std::is_same_v<T, RespInteger>) {
          return ":" + std::to_string(response.value) + "\r\n";
        } else if constexpr (std::is_same_v<T, RespFileTransfer>) {
          return "$" + std::to_string(response.bytes.size()) + "\r\n" +
                 response.bytes;
        } else if constexpr (std::is_same_v<T, RespArray>) {
          std::string encoded =
              "*" + std::to_string(response.values.size()) + "\r\n";
          for (const RespValue& item : response.values) {
            encoded += Write(item);
          }
          return encoded;
        } else {
          std::string encoded;
          for (const RespValue& item : response.values) {
            encoded += Write(item);
          }
          return encoded;
        }
      });
}

std::string RespWriter::WriteCommand(
    const std::vector<std::string>& command) {
  return Write(RespArray::BulkStrings(command));
}

std::string RespWriter::Error(std::string_view message) {
  return Write(RespErrorReply{std::string(message)});
}

std::string ToUpperAscii(std::string value) {
  for (char& ch : value) {
    ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
  }
  return value;
}

}  // namespace redis
