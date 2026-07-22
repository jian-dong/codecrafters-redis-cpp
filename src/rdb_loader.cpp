#include "redis-cpp/rdb_loader.hpp"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace redis {
namespace {

constexpr uint8_t kOpcodeEof = 0xFF;
constexpr uint8_t kOpcodeSelectDb = 0xFE;
constexpr uint8_t kOpcodeExpireTimeSeconds = 0xFD;
constexpr uint8_t kOpcodeExpireTimeMilliseconds = 0xFC;
constexpr uint8_t kOpcodeResizeDb = 0xFB;
constexpr uint8_t kOpcodeAux = 0xFA;
constexpr uint8_t kValueTypeString = 0x00;
constexpr size_t kHeaderSize = 9;
constexpr size_t kChecksumSize = 8;

struct RdbParseError {
  size_t offset = 0;
};

template <typename T>
using RdbParseResult = tl::expected<T, RdbParseError>;

struct LoadedString {
  std::string key;
  std::string value;
  std::optional<uint64_t> expires_at_milliseconds;
};

class RdbParser {
 public:
  explicit RdbParser(std::vector<uint8_t> bytes) : bytes_(std::move(bytes)) {}

  RdbParseResult<std::vector<LoadedString>> Parse() {
    RdbParseResult<void> header = ReadHeader();
    if (!header) {
      return tl::make_unexpected(header.error());
    }

    std::vector<LoadedString> records;
    std::optional<uint64_t> expires_at_milliseconds;
    while (Remaining() > 0) {
      RdbParseResult<uint8_t> opcode = ReadByte();
      if (!opcode) {
        return tl::make_unexpected(opcode.error());
      }

      switch (*opcode) {
        case kOpcodeAux: {
          RdbParseResult<std::string> key = ReadString();
          if (!key) {
            return tl::make_unexpected(key.error());
          }
          RdbParseResult<std::string> value = ReadString();
          if (!value) {
            return tl::make_unexpected(value.error());
          }
          break;
        }
        case kOpcodeResizeDb: {
          RdbParseResult<uint64_t> database_size = ReadLength();
          if (!database_size) {
            return tl::make_unexpected(database_size.error());
          }
          RdbParseResult<uint64_t> expiry_size = ReadLength();
          if (!expiry_size) {
            return tl::make_unexpected(expiry_size.error());
          }
          break;
        }
        case kOpcodeSelectDb: {
          RdbParseResult<uint64_t> database_index = ReadLength();
          if (!database_index) {
            return tl::make_unexpected(database_index.error());
          }
          expires_at_milliseconds.reset();
          break;
        }
        case kOpcodeExpireTimeMilliseconds: {
          RdbParseResult<uint64_t> expiry = ReadLittleEndian64();
          if (!expiry) {
            return tl::make_unexpected(expiry.error());
          }
          expires_at_milliseconds = *expiry;
          break;
        }
        case kOpcodeExpireTimeSeconds: {
          RdbParseResult<uint32_t> expiry = ReadLittleEndian32();
          if (!expiry) {
            return tl::make_unexpected(expiry.error());
          }
          expires_at_milliseconds = static_cast<uint64_t>(*expiry) * 1000ULL;
          break;
        }
        case kOpcodeEof:
          if (Remaining() < kChecksumSize) {
            return tl::make_unexpected(ParseError());
          }
          position_ += kChecksumSize;
          if (Remaining() != 0) {
            return tl::make_unexpected(ParseError());
          }
          return records;
        default: {
          RdbParseResult<LoadedString> record =
              ReadStringRecord(*opcode, expires_at_milliseconds);
          if (!record) {
            return tl::make_unexpected(record.error());
          }
          records.push_back(std::move(*record));
          expires_at_milliseconds.reset();
          break;
        }
      }
    }

    return tl::make_unexpected(ParseError());
  }

 private:
  [[nodiscard]] RdbParseError ParseError() const {
    return RdbParseError{.offset = position_};
  }

  RdbParseResult<void> ReadHeader() {
    if (Remaining() < kHeaderSize) {
      return tl::make_unexpected(ParseError());
    }
    const std::string_view header(
        reinterpret_cast<const char*>(bytes_.data()), kHeaderSize);
    if (!header.starts_with("REDIS") ||
        !std::all_of(header.begin() + 5, header.end(), [](char character) {
          return std::isdigit(static_cast<unsigned char>(character)) != 0;
        })) {
      return tl::make_unexpected(ParseError());
    }
    position_ += kHeaderSize;
    return {};
  }

  RdbParseResult<LoadedString> ReadStringRecord(
      uint8_t value_type,
      std::optional<uint64_t> expires_at_milliseconds) {
    if (value_type != kValueTypeString) {
      return tl::make_unexpected(ParseError());
    }

    RdbParseResult<std::string> key = ReadString();
    if (!key) {
      return tl::make_unexpected(key.error());
    }
    RdbParseResult<std::string> value = ReadString();
    if (!value) {
      return tl::make_unexpected(value.error());
    }
    return LoadedString{.key = std::move(*key),
                        .value = std::move(*value),
                        .expires_at_milliseconds = expires_at_milliseconds};
  }

  RdbParseResult<uint64_t> ReadLength() {
    RdbParseResult<uint8_t> first = ReadByte();
    if (!first) {
      return tl::make_unexpected(first.error());
    }
    return ReadLengthWithFirst(*first);
  }

  RdbParseResult<uint64_t> ReadLengthWithFirst(uint8_t first) {
    const uint8_t encoding = first >> 6;
    if (encoding == 0) {
      return static_cast<uint64_t>(first & 0x3F);
    }
    if (encoding == 1) {
      RdbParseResult<uint8_t> second = ReadByte();
      if (!second) {
        return tl::make_unexpected(second.error());
      }
      return (static_cast<uint64_t>(first & 0x3F) << 8) | *second;
    }
    if (encoding == 2) {
      RdbParseResult<uint32_t> value = ReadBigEndian32();
      if (!value) {
        return tl::make_unexpected(value.error());
      }
      return static_cast<uint64_t>(*value);
    }
    return tl::make_unexpected(ParseError());
  }

  RdbParseResult<std::string> ReadString() {
    RdbParseResult<uint8_t> first = ReadByte();
    if (!first) {
      return tl::make_unexpected(first.error());
    }

    if ((*first >> 6) != 3) {
      RdbParseResult<uint64_t> length = ReadLengthWithFirst(*first);
      if (!length || *length > Remaining() ||
          *length > std::numeric_limits<size_t>::max()) {
        return tl::make_unexpected(length ? ParseError() : length.error());
      }
      const size_t string_size = static_cast<size_t>(*length);
      std::string value(
          reinterpret_cast<const char*>(bytes_.data() + position_),
          string_size);
      position_ += string_size;
      return value;
    }

    switch (*first & 0x3F) {
      case 0: {
        RdbParseResult<uint8_t> integer = ReadByte();
        if (!integer) {
          return tl::make_unexpected(integer.error());
        }
        return std::to_string(static_cast<int8_t>(*integer));
      }
      case 1: {
        RdbParseResult<uint16_t> integer = ReadLittleEndian16();
        if (!integer) {
          return tl::make_unexpected(integer.error());
        }
        return std::to_string(static_cast<int16_t>(*integer));
      }
      case 2: {
        RdbParseResult<uint32_t> integer = ReadLittleEndian32();
        if (!integer) {
          return tl::make_unexpected(integer.error());
        }
        return std::to_string(static_cast<int32_t>(*integer));
      }
      default:
        return tl::make_unexpected(ParseError());
    }
  }

  RdbParseResult<uint8_t> ReadByte() {
    if (Remaining() < 1) {
      return tl::make_unexpected(ParseError());
    }
    return bytes_[position_++];
  }

  RdbParseResult<uint16_t> ReadLittleEndian16() {
    if (Remaining() < 2) {
      return tl::make_unexpected(ParseError());
    }
    const uint16_t value = static_cast<uint16_t>(bytes_[position_]) |
                           (static_cast<uint16_t>(bytes_[position_ + 1]) << 8);
    position_ += 2;
    return value;
  }

  RdbParseResult<uint32_t> ReadLittleEndian32() {
    if (Remaining() < 4) {
      return tl::make_unexpected(ParseError());
    }
    const uint32_t value =
        static_cast<uint32_t>(bytes_[position_]) |
        (static_cast<uint32_t>(bytes_[position_ + 1]) << 8) |
        (static_cast<uint32_t>(bytes_[position_ + 2]) << 16) |
        (static_cast<uint32_t>(bytes_[position_ + 3]) << 24);
    position_ += 4;
    return value;
  }

  RdbParseResult<uint64_t> ReadLittleEndian64() {
    if (Remaining() < 8) {
      return tl::make_unexpected(ParseError());
    }
    uint64_t value = 0;
    for (int shift = 0; shift < 64; shift += 8) {
      value |= static_cast<uint64_t>(bytes_[position_++]) << shift;
    }
    return value;
  }

  RdbParseResult<uint32_t> ReadBigEndian32() {
    if (Remaining() < 4) {
      return tl::make_unexpected(ParseError());
    }
    const uint32_t value =
        (static_cast<uint32_t>(bytes_[position_]) << 24) |
        (static_cast<uint32_t>(bytes_[position_ + 1]) << 16) |
        (static_cast<uint32_t>(bytes_[position_ + 2]) << 8) |
        static_cast<uint32_t>(bytes_[position_ + 3]);
    position_ += 4;
    return value;
  }

  [[nodiscard]] size_t Remaining() const {
    return bytes_.size() - position_;
  }

  std::vector<uint8_t> bytes_;
  size_t position_ = 0;
};

void ApplyRecords(const std::vector<LoadedString>& records,
                  Database& database) {
  const int64_t signed_now =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now().time_since_epoch())
          .count();
  const uint64_t now = signed_now < 0 ? 0 : static_cast<uint64_t>(signed_now);

  for (const LoadedString& record : records) {
    if (!record.expires_at_milliseconds.has_value()) {
      database.SetString(record.key, record.value);
      continue;
    }
    if (*record.expires_at_milliseconds <= now) {
      continue;
    }
    const uint64_t unsigned_ttl = *record.expires_at_milliseconds - now;
    const int64_t ttl = unsigned_ttl >
                                static_cast<uint64_t>(
                                    std::numeric_limits<int64_t>::max())
                            ? std::numeric_limits<int64_t>::max()
                            : static_cast<int64_t>(unsigned_ttl);
    database.SetString(record.key, record.value, std::chrono::milliseconds(ttl));
  }
}

}  // namespace

Status LoadDatabaseFromRdb(const ServerConfig& config, Database& database) {
  if (config.dir.empty() || config.dbfilename.empty()) {
    return {};
  }

  const std::filesystem::path path =
      std::filesystem::path(config.dir) / config.dbfilename;
  std::error_code status_error;
  const std::filesystem::file_status status =
      std::filesystem::status(path, status_error);
  if (status_error == std::errc::no_such_file_or_directory ||
      status.type() == std::filesystem::file_type::not_found) {
    return {};
  }
  if (status_error || status.type() != std::filesystem::file_type::regular) {
    return tl::make_unexpected(
        MakeFileSystemError(FileSystemErrorCode::kReadFileFailed,
                            path.string()));
  }

  std::ifstream file(path, std::ios::binary);
  if (!file.is_open()) {
    return tl::make_unexpected(
        MakeFileSystemError(FileSystemErrorCode::kReadFileFailed,
                            path.string()));
  }
  std::vector<uint8_t> bytes((std::istreambuf_iterator<char>(file)),
                             std::istreambuf_iterator<char>());
  if (file.bad()) {
    return tl::make_unexpected(
        MakeFileSystemError(FileSystemErrorCode::kReadFileFailed,
                            path.string()));
  }

  RdbParser parser(std::move(bytes));
  RdbParseResult<std::vector<LoadedString>> records = parser.Parse();
  if (!records) {
    return tl::make_unexpected(
        MakeFileSystemError(FileSystemErrorCode::kInvalidFileFormat,
                            path.string()));
  }
  ApplyRecords(*records, database);
  return {};
}

}  // namespace redis
