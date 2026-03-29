#include "redis-cpp/rdb_loader.hpp"

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <optional>
#include <string>
#include <string_view>
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

class RdbParser {
 public:
  explicit RdbParser(std::vector<uint8_t> bytes) : bytes_(std::move(bytes)) {}

  bool Parse(Database& database) {
    if (!ReadHeader()) {
      return false;
    }

    std::optional<uint64_t> expires_at_ms;
    while (remaining() > 0) {
      uint8_t opcode = 0;
      if (!ReadByte(opcode)) {
        return false;
      }

      switch (opcode) {
        case kOpcodeAux: {
          std::string key;
          std::string value;
          if (!ReadString(key) || !ReadString(value)) {
            return false;
          }
          continue;
        }
        case kOpcodeResizeDb: {
          uint64_t db_size = 0;
          uint64_t expires_size = 0;
          if (!ReadLength(db_size) || !ReadLength(expires_size)) {
            return false;
          }
          continue;
        }
        case kOpcodeSelectDb: {
          uint64_t db_index = 0;
          if (!ReadLength(db_index)) {
            return false;
          }
          expires_at_ms.reset();
          continue;
        }
        case kOpcodeExpireTimeMilliseconds: {
          uint64_t value = 0;
          if (!ReadLittleEndian64(value)) {
            return false;
          }
          expires_at_ms = value;
          continue;
        }
        case kOpcodeExpireTimeSeconds: {
          uint32_t value = 0;
          if (!ReadLittleEndian32(value)) {
            return false;
          }
          expires_at_ms = static_cast<uint64_t>(value) * 1000ULL;
          continue;
        }
        case kOpcodeEof:
          return true;
        default:
          if (!ReadKeyValuePair(opcode, expires_at_ms, database)) {
            return false;
          }
          expires_at_ms.reset();
          break;
      }
    }

    return true;
  }

 private:
  bool ReadHeader() {
    if (remaining() < 9) {
      return false;
    }

    const std::string_view magic(
        reinterpret_cast<const char*>(bytes_.data()), 5);
    if (magic != "REDIS") {
      return false;
    }

    position_ += 9;
    return true;
  }

  bool ReadKeyValuePair(uint8_t value_type,
                        const std::optional<uint64_t>& expires_at_ms,
                        Database& database) {
    std::string key;
    if (!ReadString(key)) {
      return false;
    }

    if (value_type != kValueTypeString) {
      return false;
    }

    std::string value;
    if (!ReadString(value)) {
      return false;
    }

    if (!expires_at_ms.has_value()) {
      database.SetString(key, std::move(value));
      return true;
    }

    const auto now = std::chrono::time_point_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now());
    const uint64_t now_ms =
        static_cast<uint64_t>(now.time_since_epoch().count());
    if (*expires_at_ms <= now_ms) {
      return true;
    }

    database.SetString(
        key, std::move(value),
        std::chrono::milliseconds(*expires_at_ms - now_ms));
    return true;
  }

  bool ReadLength(uint64_t& length) {
    uint8_t first = 0;
    if (!ReadByte(first)) {
      return false;
    }

    const uint8_t encoding = first >> 6;
    if (encoding == 0) {
      length = first & 0x3F;
      return true;
    }
    if (encoding == 1) {
      uint8_t second = 0;
      if (!ReadByte(second)) {
        return false;
      }
      length = (static_cast<uint64_t>(first & 0x3F) << 8) | second;
      return true;
    }
    if (encoding == 2) {
      uint32_t value = 0;
      if (!ReadBigEndian32(value)) {
        return false;
      }
      length = value;
      return true;
    }

    return false;
  }

  bool ReadString(std::string& value) {
    uint8_t first = 0;
    if (!ReadByte(first)) {
      return false;
    }

    const uint8_t encoding = first >> 6;
    if (encoding != 3) {
      --position_;
      uint64_t length = 0;
      if (!ReadLength(length) || remaining() < length) {
        return false;
      }

      value.assign(reinterpret_cast<const char*>(bytes_.data() + position_),
                   static_cast<size_t>(length));
      position_ += static_cast<size_t>(length);
      return true;
    }

    switch (first & 0x3F) {
      case 0: {
        uint8_t integer = 0;
        if (!ReadByte(integer)) {
          return false;
        }
        value = std::to_string(static_cast<int8_t>(integer));
        return true;
      }
      case 1: {
        uint16_t integer = 0;
        if (!ReadLittleEndian16(integer)) {
          return false;
        }
        value = std::to_string(static_cast<int16_t>(integer));
        return true;
      }
      case 2: {
        uint32_t integer = 0;
        if (!ReadLittleEndian32(integer)) {
          return false;
        }
        value = std::to_string(static_cast<int32_t>(integer));
        return true;
      }
      default:
        return false;
    }
  }

  bool ReadByte(uint8_t& value) {
    if (remaining() < 1) {
      return false;
    }

    value = bytes_[position_++];
    return true;
  }

  bool ReadLittleEndian16(uint16_t& value) {
    if (remaining() < 2) {
      return false;
    }

    value = static_cast<uint16_t>(bytes_[position_]) |
            (static_cast<uint16_t>(bytes_[position_ + 1]) << 8);
    position_ += 2;
    return true;
  }

  bool ReadLittleEndian32(uint32_t& value) {
    if (remaining() < 4) {
      return false;
    }

    value = static_cast<uint32_t>(bytes_[position_]) |
            (static_cast<uint32_t>(bytes_[position_ + 1]) << 8) |
            (static_cast<uint32_t>(bytes_[position_ + 2]) << 16) |
            (static_cast<uint32_t>(bytes_[position_ + 3]) << 24);
    position_ += 4;
    return true;
  }

  bool ReadLittleEndian64(uint64_t& value) {
    if (remaining() < 8) {
      return false;
    }

    value = static_cast<uint64_t>(bytes_[position_]) |
            (static_cast<uint64_t>(bytes_[position_ + 1]) << 8) |
            (static_cast<uint64_t>(bytes_[position_ + 2]) << 16) |
            (static_cast<uint64_t>(bytes_[position_ + 3]) << 24) |
            (static_cast<uint64_t>(bytes_[position_ + 4]) << 32) |
            (static_cast<uint64_t>(bytes_[position_ + 5]) << 40) |
            (static_cast<uint64_t>(bytes_[position_ + 6]) << 48) |
            (static_cast<uint64_t>(bytes_[position_ + 7]) << 56);
    position_ += 8;
    return true;
  }

  bool ReadBigEndian32(uint32_t& value) {
    if (remaining() < 4) {
      return false;
    }

    value = (static_cast<uint32_t>(bytes_[position_]) << 24) |
            (static_cast<uint32_t>(bytes_[position_ + 1]) << 16) |
            (static_cast<uint32_t>(bytes_[position_ + 2]) << 8) |
            static_cast<uint32_t>(bytes_[position_ + 3]);
    position_ += 4;
    return true;
  }

  size_t remaining() const { return bytes_.size() - position_; }

  std::vector<uint8_t> bytes_;
  size_t position_ = 0;
};

}  // namespace

void LoadDatabaseFromRdb(const ServerConfig& config, Database& database) {
  if (config.dir.empty() || config.dbfilename.empty()) {
    return;
  }

  const std::filesystem::path path =
      std::filesystem::path(config.dir) / config.dbfilename;
  std::error_code error_code;
  if (!std::filesystem::exists(path, error_code) || error_code) {
    return;
  }

  std::ifstream file(path, std::ios::binary);
  if (!file) {
    return;
  }

  std::vector<uint8_t> bytes((std::istreambuf_iterator<char>(file)),
                             std::istreambuf_iterator<char>());
  if (bytes.empty()) {
    return;
  }

  RdbParser parser(std::move(bytes));
  (void)parser.Parse(database);
}

}  // namespace redis
