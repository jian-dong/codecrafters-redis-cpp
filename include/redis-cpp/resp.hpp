#pragma once

#include <cstdint>
#include <initializer_list>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
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

struct RespErrorReply {
  std::string value;
};

struct RespNullBulk {};
struct RespNullArray {};

struct RespInteger {
  int64_t value = 0;
};

struct RespFileTransfer {
  std::string bytes;
};

class RespValue;

struct RespArray {
  RespArray() = default;
  explicit RespArray(std::vector<RespValue> values);

  [[nodiscard]] static RespArray BulkStrings(std::vector<std::string> values);

  std::vector<RespValue> values;
};

struct RespSequence {
  RespSequence() = default;
  RespSequence(std::vector<RespValue> values);
  RespSequence(std::initializer_list<RespValue> values);

  std::vector<RespValue> values;
};

using RespValueStorage =
    std::variant<RespSimpleString, RespBulkString, RespErrorReply, RespNullBulk,
                 RespNullArray, RespInteger, RespFileTransfer, RespArray,
                 RespSequence>;

class RespValue {
 public:
  RespValue(RespSimpleString value) : storage_(std::move(value)) {}
  RespValue(RespBulkString value) : storage_(std::move(value)) {}
  RespValue(RespErrorReply value) : storage_(std::move(value)) {}
  RespValue(RespNullBulk value) : storage_(std::move(value)) {}
  RespValue(RespNullArray value) : storage_(std::move(value)) {}
  RespValue(RespInteger value) : storage_(std::move(value)) {}
  RespValue(RespFileTransfer value) : storage_(std::move(value)) {}
  RespValue(RespArray value) : storage_(std::move(value)) {}
  RespValue(RespSequence value) : storage_(std::move(value)) {}

  template <typename T>
  [[nodiscard]] bool Is() const {
    return std::holds_alternative<T>(storage_);
  }

  template <typename T>
  [[nodiscard]] const T& Get() const {
    return std::get<T>(storage_);
  }

  template <typename T>
  [[nodiscard]] T& Get() {
    return std::get<T>(storage_);
  }

  template <typename Visitor>
  decltype(auto) Visit(Visitor&& visitor) {
    return std::visit(std::forward<Visitor>(visitor), storage_);
  }

  template <typename Visitor>
  decltype(auto) Visit(Visitor&& visitor) const {
    return std::visit(std::forward<Visitor>(visitor), storage_);
  }

 private:
  RespValueStorage storage_;
};

class RespParser {
 public:
  enum class InputMode {
    kClient,
    kRespOnly,
  };

  explicit RespParser(InputMode input_mode = InputMode::kClient)
      : input_mode_(input_mode) {}

  void Append(std::string_view data);
  Result<std::optional<std::vector<std::string>>> NextCommand();
  size_t BufferSize() const { return buffer_.size() - consumed_; }

 private:
  InputMode input_mode_;
  std::string buffer_;
  size_t consumed_ = 0;
};

class RespWriter {
 public:
  static std::string Write(const RespValue& value);
  static std::string WriteCommand(const std::vector<std::string>& command);
  static std::string Error(std::string_view message);
};

std::string ToUpperAscii(std::string value);

}  // namespace redis
