#pragma once

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "redis-cpp/result.hpp"

namespace redis {

struct RespSimpleString {
  std::string value;
};

struct RespBulkString {
  std::string value;
};

struct RespNullBulk {};

struct RespNullArray {};

struct RespInteger {
  int64_t value = 0;
};

struct RespArray {
  std::vector<std::string> values;
};

struct RespRaw {
  std::string encoded;
};

using RespValue =
    std::variant<RespSimpleString, RespBulkString, RespNullBulk, RespNullArray,
                 RespInteger, RespArray, RespRaw>;

class RespParser {
 public:
  void Append(std::string_view data);
  Result<std::optional<std::vector<std::string>>> NextCommand();
  size_t BufferSize() const { return buffer_.size(); }

 private:
  std::string buffer_;
};

class RespWriter {
 public:
  static std::string Write(const RespValue& value);
  static std::string Error(std::string_view message);
};

std::string ToUpperAscii(std::string value);
bool ParseMilliseconds(std::string_view data, int64_t& value);
bool ParseSignedInteger(std::string_view data, int64_t& value);
bool ParseTimeoutDuration(std::string_view data,
                          std::chrono::steady_clock::duration& timeout);

}  // namespace redis
