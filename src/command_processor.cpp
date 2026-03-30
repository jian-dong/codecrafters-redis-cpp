#include "redis-cpp/command_processor.hpp"

#include <chrono>
#include <array>
#include <cerrno>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <optional>
#include <sstream>
#include <utility>
#include <vector>

#include "redis-cpp/resp.hpp"

namespace redis {
namespace {

constexpr double kMinLongitude = -180.0;
constexpr double kMaxLongitude = 180.0;
constexpr double kMinLatitude = -85.05112878;
constexpr double kMaxLatitude = 85.05112878;
constexpr int kGeoStepBits = 26;
constexpr double kEarthRadiusMeters = 6372797.560856;
constexpr double kDegreesToRadians = 3.14159265358979323846 / 180.0;
constexpr double kMetersPerKilometer = 1000.0;
constexpr double kMetersPerMile = 1609.344;
constexpr double kMetersPerFoot = 0.3048;

constexpr std::string_view kMasterReplId =
    "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";

const std::string& GetEmptyRdb() {
  static const std::string rdb = []() {
    // Empty RDB file (Redis 7.2 format)
    static const uint8_t kBytes[] = {
        0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, 0xfa, 0x09,
        0x72, 0x65, 0x64, 0x69, 0x73, 0x2d, 0x76, 0x65, 0x72, 0x05, 0x37,
        0x2e, 0x32, 0x2e, 0x30, 0xfa, 0x0a, 0x72, 0x65, 0x64, 0x69, 0x73,
        0x2d, 0x62, 0x69, 0x74, 0x73, 0xc0, 0x40, 0xfa, 0x05, 0x63, 0x74,
        0x69, 0x6d, 0x65, 0xc2, 0x6d, 0x08, 0xbc, 0x65, 0xfa, 0x08, 0x75,
        0x73, 0x65, 0x64, 0x2d, 0x6d, 0x65, 0x6d, 0xc2, 0xb0, 0xc4, 0x10,
        0x00, 0xfa, 0x08, 0x61, 0x6f, 0x66, 0x2d, 0x62, 0x61, 0x73, 0x65,
        0xc0, 0x00, 0xff, 0xf0, 0x6e, 0x3b, 0xfe, 0xa0, 0xff, 0x5a, 0xa2};
    return std::string(reinterpret_cast<const char*>(kBytes), sizeof(kBytes));
  }();
  return rdb;
}

std::string EncodeStreamRange(
    const std::vector<Database::StreamRangeEntry>& entries) {
  std::string encoded = "*" + std::to_string(entries.size()) + "\r\n";
  for (const Database::StreamRangeEntry& entry : entries) {
    encoded += "*2\r\n";
    encoded += RespWriter::Write(RespBulkString{entry.id});
    encoded += "*" + std::to_string(entry.values.size()) + "\r\n";
    for (const std::string& value : entry.values) {
      encoded += RespWriter::Write(RespBulkString{value});
    }
  }
  return encoded;
}

std::string EncodeXreadResponse(
    const std::vector<std::pair<std::string, std::vector<Database::StreamRangeEntry>>>&
        streams) {
  std::string encoded = "*" + std::to_string(streams.size()) + "\r\n";
  for (const auto& [key, entries] : streams) {
    encoded += "*2\r\n";
    encoded += RespWriter::Write(RespBulkString{key});
    encoded += EncodeStreamRange(entries);
  }
  return encoded;
}

bool ParseDouble(std::string_view data, double& value) {
  if (data.empty()) {
    return false;
  }

  errno = 0;
  char* parse_end = nullptr;
  const std::string text(data);
  value = std::strtod(text.c_str(), &parse_end);
  return parse_end == text.c_str() + text.size() && errno != ERANGE;
}

uint64_t EncodeGeoScore(double longitude, double latitude) {
  double longitude_min = kMinLongitude;
  double longitude_max = kMaxLongitude;
  double latitude_min = kMinLatitude;
  double latitude_max = kMaxLatitude;

  uint64_t score = 0;
  for (int bit = 0; bit < kGeoStepBits; ++bit) {
    const double longitude_mid = (longitude_min + longitude_max) / 2.0;
    score <<= 1;
    if (longitude >= longitude_mid) {
      score |= 1;
      longitude_min = longitude_mid;
    } else {
      longitude_max = longitude_mid;
    }

    const double latitude_mid = (latitude_min + latitude_max) / 2.0;
    score <<= 1;
    if (latitude >= latitude_mid) {
      score |= 1;
      latitude_min = latitude_mid;
    } else {
      latitude_max = latitude_mid;
    }
  }

  return score;
}

std::pair<double, double> DecodeGeoScore(uint64_t score) {
  double longitude_min = kMinLongitude;
  double longitude_max = kMaxLongitude;
  double latitude_min = kMinLatitude;
  double latitude_max = kMaxLatitude;

  for (int bit = 0; bit < kGeoStepBits; ++bit) {
    const int longitude_shift = ((kGeoStepBits - 1 - bit) * 2) + 1;
    const int latitude_shift = (kGeoStepBits - 1 - bit) * 2;
    const uint64_t longitude_bit = (score >> longitude_shift) & 1ULL;
    const uint64_t latitude_bit = (score >> latitude_shift) & 1ULL;

    const double longitude_mid = (longitude_min + longitude_max) / 2.0;
    if (longitude_bit != 0) {
      longitude_min = longitude_mid;
    } else {
      longitude_max = longitude_mid;
    }

    const double latitude_mid = (latitude_min + latitude_max) / 2.0;
    if (latitude_bit != 0) {
      latitude_min = latitude_mid;
    } else {
      latitude_max = latitude_mid;
    }
  }

  return {(longitude_min + longitude_max) / 2.0,
          (latitude_min + latitude_max) / 2.0};
}

std::string FormatGeoCoordinate(double value) {
  std::ostringstream stream;
  stream << std::setprecision(17) << value;
  return stream.str();
}

double GeoDistanceMeters(double longitude_a, double latitude_a, double longitude_b,
                         double latitude_b) {
  const double latitude_a_radians = latitude_a * kDegreesToRadians;
  const double latitude_b_radians = latitude_b * kDegreesToRadians;
  const double delta_latitude_radians =
      (latitude_b - latitude_a) * kDegreesToRadians;
  const double delta_longitude_radians =
      (longitude_b - longitude_a) * kDegreesToRadians;

  const double haversine =
      std::sin(delta_latitude_radians / 2.0) *
          std::sin(delta_latitude_radians / 2.0) +
      std::cos(latitude_a_radians) * std::cos(latitude_b_radians) *
          std::sin(delta_longitude_radians / 2.0) *
          std::sin(delta_longitude_radians / 2.0);
  return 2.0 * kEarthRadiusMeters * std::asin(std::sqrt(haversine));
}

bool DecodeGeoMember(Database& database, const std::string& key,
                     const std::string& member, double& longitude,
                     double& latitude, bool& wrong_type) {
  const Database::ZScoreResult result = database.ZScore(key, member);
  if (result.wrong_type) {
    wrong_type = true;
    return false;
  }
  if (!result.found) {
    return false;
  }

  double score = 0.0;
  if (!ParseDouble(result.score, score) || score < 0.0) {
    return false;
  }

  const auto decoded = DecodeGeoScore(static_cast<uint64_t>(score));
  longitude = decoded.first;
  latitude = decoded.second;
  return true;
}

bool ParseGeoDistanceMeters(std::string_view radius_text, std::string_view unit_text,
                            double& radius_meters) {
  double radius = 0.0;
  if (!ParseDouble(radius_text, radius)) {
    return false;
  }

  const std::string unit = ToUpperAscii(std::string(unit_text));
  double multiplier = 0.0;
  if (unit == "M") {
    multiplier = 1.0;
  } else if (unit == "KM") {
    multiplier = kMetersPerKilometer;
  } else if (unit == "MI") {
    multiplier = kMetersPerMile;
  } else if (unit == "FT") {
    multiplier = kMetersPerFoot;
  } else {
    return false;
  }

  radius_meters = radius * multiplier;
  return true;
}

uint32_t RotateRight(uint32_t value, int shift) {
  return (value >> shift) | (value << (32 - shift));
}

std::string Sha256Hex(std::string_view input) {
  static constexpr std::array<uint32_t, 64> kRoundConstants = {
      0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b,
      0x59f111f1, 0x923f82a4, 0xab1c5ed5, 0xd807aa98, 0x12835b01,
      0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7,
      0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc,
      0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152,
      0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147,
      0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
      0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
      0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819,
      0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116, 0x1e376c08,
      0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f,
      0x682e6ff3, 0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
      0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
  };
  static constexpr std::array<uint32_t, 8> kInitialState = {
      0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
      0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
  };

  std::vector<uint8_t> message(input.begin(), input.end());
  const uint64_t bit_length = static_cast<uint64_t>(message.size()) * 8ULL;
  message.push_back(0x80);
  while ((message.size() % 64) != 56) {
    message.push_back(0);
  }
  for (int shift = 56; shift >= 0; shift -= 8) {
    message.push_back(static_cast<uint8_t>((bit_length >> shift) & 0xffU));
  }

  std::array<uint32_t, 8> state = kInitialState;
  std::array<uint32_t, 64> schedule = {};
  for (size_t offset = 0; offset < message.size(); offset += 64) {
    for (size_t index = 0; index < 16; ++index) {
      const size_t base = offset + (index * 4);
      schedule[index] = (static_cast<uint32_t>(message[base]) << 24) |
                        (static_cast<uint32_t>(message[base + 1]) << 16) |
                        (static_cast<uint32_t>(message[base + 2]) << 8) |
                        static_cast<uint32_t>(message[base + 3]);
    }
    for (size_t index = 16; index < schedule.size(); ++index) {
      const uint32_t s0 = RotateRight(schedule[index - 15], 7) ^
                          RotateRight(schedule[index - 15], 18) ^
                          (schedule[index - 15] >> 3);
      const uint32_t s1 = RotateRight(schedule[index - 2], 17) ^
                          RotateRight(schedule[index - 2], 19) ^
                          (schedule[index - 2] >> 10);
      schedule[index] =
          schedule[index - 16] + s0 + schedule[index - 7] + s1;
    }

    uint32_t a = state[0];
    uint32_t b = state[1];
    uint32_t c = state[2];
    uint32_t d = state[3];
    uint32_t e = state[4];
    uint32_t f = state[5];
    uint32_t g = state[6];
    uint32_t h = state[7];

    for (size_t index = 0; index < schedule.size(); ++index) {
      const uint32_t sum1 =
          RotateRight(e, 6) ^ RotateRight(e, 11) ^ RotateRight(e, 25);
      const uint32_t choice = (e & f) ^ ((~e) & g);
      const uint32_t temp1 =
          h + sum1 + choice + kRoundConstants[index] + schedule[index];
      const uint32_t sum0 =
          RotateRight(a, 2) ^ RotateRight(a, 13) ^ RotateRight(a, 22);
      const uint32_t majority = (a & b) ^ (a & c) ^ (b & c);
      const uint32_t temp2 = sum0 + majority;

      h = g;
      g = f;
      f = e;
      e = d + temp1;
      d = c;
      c = b;
      b = a;
      a = temp1 + temp2;
    }

    state[0] += a;
    state[1] += b;
    state[2] += c;
    state[3] += d;
    state[4] += e;
    state[5] += f;
    state[6] += g;
    state[7] += h;
  }

  std::ostringstream output;
  output << std::hex << std::setfill('0');
  for (uint32_t value : state) {
    output << std::setw(8) << value;
  }
  return output.str();
}

std::string EncodeAclGetuserResponse(bool nopass,
                                     const std::vector<std::string>& passwords) {
  std::string response = "*4\r\n";
  response += RespWriter::Write(RespBulkString{"flags"});
  if (nopass) {
    response += "*1\r\n";
    response += RespWriter::Write(RespBulkString{"nopass"});
  } else {
    response += "*0\r\n";
  }
  response += RespWriter::Write(RespBulkString{"passwords"});
  response += "*" + std::to_string(passwords.size()) + "\r\n";
  for (const std::string& password : passwords) {
    response += RespWriter::Write(RespBulkString{password});
  }
  return response;
}

}  // namespace

CommandProcessor::CommandProcessor(Database& database, bool is_replica,
                                   ReplicaManager* replica_manager,
                                   const ServerConfig* server_config)
    : database_(database),
      is_replica_(is_replica),
      replica_manager_(replica_manager),
      server_config_(server_config) {}

bool CommandProcessor::DefaultUserStartsAuthenticated() const {
  std::lock_guard<std::mutex> lock(acl_mutex_);
  return default_user_.nopass;
}

std::string CommandErrorMessage(const CommandError& error) {
  switch (error.code) {
    case CommandErrorCode::kUnknownCommand:
      return "ERR unknown command '" + error.command + "'";
    case CommandErrorCode::kWrongArity:
      return "ERR wrong number of arguments for '" + error.command +
             "' command";
    case CommandErrorCode::kWrongType:
      return "WRONGTYPE Operation against a key holding the wrong kind of "
             "value";
    case CommandErrorCode::kSyntaxError:
      return "ERR syntax error";
    case CommandErrorCode::kInvalidInteger:
      return "ERR value is not an integer or out of range";
    case CommandErrorCode::kInvalidGeoCoordinates:
      return "ERR invalid longitude,latitude pair";
    case CommandErrorCode::kWrongPass:
      return "WRONGPASS invalid username-password pair or user is disabled.";
    case CommandErrorCode::kXaddIdNotGreaterThanZeroZero:
      return "ERR The ID specified in XADD must be greater than 0-0";
    case CommandErrorCode::kXaddIdNotGreaterThanTopItem:
      return "ERR The ID specified in XADD is equal or smaller than the "
             "target stream top item";
    case CommandErrorCode::kExecWithoutMulti:
      return "ERR EXEC without MULTI";
    case CommandErrorCode::kDiscardWithoutMulti:
      return "ERR DISCARD without MULTI";
  }

  return "ERR command failed";
}

CommandResult CommandProcessor::Execute(const std::vector<std::string>& args) {
  if (args.empty()) {
    return RespSimpleString{"PONG"};
  }

  const std::string command = ToUpperAscii(args[0]);
  if (command == "PING") {
    return HandlePing(args);
  }
  if (command == "ECHO") {
    return HandleEcho(args);
  }
  if (command == "SET") {
    return HandleSet(args);
  }
  if (command == "GET") {
    return HandleGet(args);
  }
  if (command == "KEYS") {
    return HandleKeys(args);
  }
  if (command == "AUTH") {
    return HandleAuth(args);
  }
  if (command == "ACL") {
    return HandleAcl(args);
  }
  if (command == "SUBSCRIBE") {
    return HandleSubscribe(args);
  }
  if (command == "TYPE") {
    return HandleType(args);
  }
  if (command == "GEOADD") {
    return HandleGeoadd(args);
  }
  if (command == "GEOPOS") {
    return HandleGeopos(args);
  }
  if (command == "GEODIST") {
    return HandleGeodist(args);
  }
  if (command == "GEOSEARCH") {
    return HandleGeosearch(args);
  }
  if (command == "ZADD") {
    return HandleZadd(args);
  }
  if (command == "ZRANK") {
    return HandleZrank(args);
  }
  if (command == "ZRANGE") {
    return HandleZrange(args);
  }
  if (command == "ZCARD") {
    return HandleZcard(args);
  }
  if (command == "ZSCORE") {
    return HandleZscore(args);
  }
  if (command == "ZREM") {
    return HandleZrem(args);
  }
  if (command == "XADD") {
    return HandleXadd(args);
  }
  if (command == "XRANGE") {
    return HandleXrange(args);
  }
  if (command == "XREAD") {
    return HandleXread(args);
  }
  if (command == "RPUSH") {
    return HandleRpush(args);
  }
  if (command == "LPUSH") {
    return HandleLpush(args);
  }
  if (command == "LRANGE") {
    return HandleLrange(args);
  }
  if (command == "LLEN") {
    return HandleLlen(args);
  }
  if (command == "LPOP") {
    return HandleLpop(args);
  }
  if (command == "BLPOP") {
    return HandleBlpop(args);
  }
  if (command == "INCR") {
    return HandleIncr(args);
  }
  if (command == "CONFIG") {
    return HandleConfig(args);
  }
  if (command == "INFO") {
    return HandleInfo(args);
  }
  if (command == "REPLCONF") {
    return HandleReplconf(args);
  }
  if (command == "WAIT") {
    return HandleWait(args);
  }
  if (command == "PSYNC") {
    const std::string& rdb = GetEmptyRdb();
    std::string response =
        "+FULLRESYNC " + std::string(kMasterReplId) + " 0\r\n";
    response += "$" + std::to_string(rdb.size()) + "\r\n";
    response += rdb;
    return RespRaw{std::move(response)};
  }
  if (command == "EXEC") {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kExecWithoutMulti,
                     .command = "exec"});
  }
  if (command == "DISCARD") {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kDiscardWithoutMulti,
                     .command = "discard"});
  }

  return tl::make_unexpected(CommandError{
      .code = CommandErrorCode::kUnknownCommand, .command = args[0]});
}

CommandResult CommandProcessor::HandlePing(
    const std::vector<std::string>& args) {
  if (args.size() >= 2) {
    return RespBulkString{args[1]};
  }

  return RespSimpleString{"PONG"};
}

CommandResult CommandProcessor::HandleEcho(
    const std::vector<std::string>& args) {
  if (args.size() < 2) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "echo"});
  }

  return RespBulkString{args[1]};
}

CommandResult CommandProcessor::HandleSet(
    const std::vector<std::string>& args) {
  if (args.size() < 3) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "set"});
  }

  std::optional<std::chrono::milliseconds> ttl;
  if (args.size() == 5) {
    if (ToUpperAscii(args[3]) != "PX") {
      return tl::make_unexpected(CommandError{
          .code = CommandErrorCode::kSyntaxError, .command = "set"});
    }

    int64_t ttl_milliseconds = 0;
    if (!ParseMilliseconds(args[4], ttl_milliseconds)) {
      return tl::make_unexpected(CommandError{
          .code = CommandErrorCode::kInvalidInteger, .command = "set"});
    }

    ttl = std::chrono::milliseconds(ttl_milliseconds);
  } else if (args.size() != 3) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kSyntaxError, .command = "set"});
  }

  database_.SetString(args[1], args[2], ttl);
  return RespSimpleString{"OK"};
}

CommandResult CommandProcessor::HandleGet(
    const std::vector<std::string>& args) {
  if (args.size() < 2) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "get"});
  }

  const Database::StringLookup result = database_.GetString(args[1]);
  if (result.type == ValueType::kNone) {
    return RespNullBulk{};
  }
  if (result.type != ValueType::kString) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongType, .command = "get"});
  }

  return RespBulkString{*result.value};
}

CommandResult CommandProcessor::HandleKeys(
    const std::vector<std::string>& args) {
  if (args.size() != 2) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "keys"});
  }

  if (args[1] != "*") {
    return RespArray{{}};
  }

  return RespArray{database_.Keys()};
}

CommandResult CommandProcessor::HandleAuth(
    const std::vector<std::string>& args) {
  if (args.size() != 3) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "auth"});
  }

  if (args[1] != "default") {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongPass, .command = "auth"});
  }

  const std::string password_hash = Sha256Hex(args[2]);

  std::lock_guard<std::mutex> lock(acl_mutex_);
  if (default_user_.nopass) {
    return RespSimpleString{"OK"};
  }

  for (const std::string& stored_hash : default_user_.password_hashes) {
    if (stored_hash == password_hash) {
      return RespSimpleString{"OK"};
    }
  }

  return tl::make_unexpected(
      CommandError{.code = CommandErrorCode::kWrongPass, .command = "auth"});
}

CommandResult CommandProcessor::HandleAcl(
    const std::vector<std::string>& args) {
  if (args.size() == 2 && ToUpperAscii(args[1]) == "WHOAMI") {
    return RespBulkString{"default"};
  }

  if (args.size() == 3 && ToUpperAscii(args[1]) == "GETUSER") {
    if (args[2] != "default") {
      return RespNullBulk{};
    }

    std::lock_guard<std::mutex> lock(acl_mutex_);
    return RespRaw{
        EncodeAclGetuserResponse(default_user_.nopass,
                                 default_user_.password_hashes)};
  }

  if (args.size() == 4 && ToUpperAscii(args[1]) == "SETUSER") {
    if (args[2] != "default" || !args[3].starts_with(">")) {
      return tl::make_unexpected(
          CommandError{.code = CommandErrorCode::kSyntaxError, .command = "acl"});
    }

    std::lock_guard<std::mutex> lock(acl_mutex_);
    default_user_.nopass = false;
    default_user_.password_hashes = {Sha256Hex(std::string_view(args[3]).substr(1))};
    return RespSimpleString{"OK"};
  }

  if (args.size() == 2 || args.size() == 3) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kSyntaxError, .command = "acl"});
  }

  return tl::make_unexpected(
      CommandError{.code = CommandErrorCode::kWrongArity, .command = "acl"});
}

CommandResult CommandProcessor::HandleSubscribe(
    const std::vector<std::string>& args) {
  if (args.size() != 2) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kWrongArity, .command = "subscribe"});
  }

  std::string response = "*3\r\n";
  response += "$9\r\nsubscribe\r\n";
  response += "$" + std::to_string(args[1].size()) + "\r\n" + args[1] + "\r\n";
  response += ":1\r\n";
  return RespRaw{std::move(response)};
}

CommandResult CommandProcessor::HandleType(
    const std::vector<std::string>& args) {
  if (args.size() < 2) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "type"});
  }

  return RespSimpleString{ValueTypeName(database_.TypeOf(args[1]))};
}

CommandResult CommandProcessor::HandleGeoadd(
    const std::vector<std::string>& args) {
  if (args.size() != 5) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "geoadd"});
  }

  double longitude = 0.0;
  double latitude = 0.0;
  const bool longitude_valid =
      ParseDouble(args[2], longitude) && longitude >= kMinLongitude &&
      longitude <= kMaxLongitude;
  const bool latitude_valid =
      ParseDouble(args[3], latitude) && latitude >= kMinLatitude &&
      latitude <= kMaxLatitude;
  if (!longitude_valid || !latitude_valid) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kInvalidGeoCoordinates, .command = "geoadd"});
  }

  const uint64_t geo_score = EncodeGeoScore(longitude, latitude);
  const Database::ZAddResult result = database_.ZAdd(
      args[1], static_cast<double>(geo_score), std::to_string(geo_score),
      args[4]);
  if (result.wrong_type) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongType, .command = "geoadd"});
  }

  return RespInteger{result.added};
}

CommandResult CommandProcessor::HandleGeopos(
    const std::vector<std::string>& args) {
  if (args.size() < 3) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "geopos"});
  }

  std::string response = "*" + std::to_string(args.size() - 2) + "\r\n";
  for (size_t index = 2; index < args.size(); ++index) {
    const Database::ZScoreResult result = database_.ZScore(args[1], args[index]);
    if (result.wrong_type) {
      return tl::make_unexpected(
          CommandError{.code = CommandErrorCode::kWrongType, .command = "geopos"});
    }

    if (!result.found) {
      response += "*-1\r\n";
      continue;
    }

    double score = 0.0;
    if (!ParseDouble(result.score, score) || score < 0.0) {
      response += "*-1\r\n";
      continue;
    }

    const auto [longitude, latitude] =
        DecodeGeoScore(static_cast<uint64_t>(score));
    response += "*2\r\n";
    response +=
        RespWriter::Write(RespBulkString{FormatGeoCoordinate(longitude)});
    response += RespWriter::Write(RespBulkString{FormatGeoCoordinate(latitude)});
  }

  return RespRaw{std::move(response)};
}

CommandResult CommandProcessor::HandleGeodist(
    const std::vector<std::string>& args) {
  if (args.size() != 4) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "geodist"});
  }

  bool wrong_type = false;
  double longitude_a = 0.0;
  double latitude_a = 0.0;
  if (!DecodeGeoMember(database_, args[1], args[2], longitude_a, latitude_a,
                       wrong_type)) {
    if (wrong_type) {
      return tl::make_unexpected(
          CommandError{.code = CommandErrorCode::kWrongType, .command = "geodist"});
    }
    return RespNullBulk{};
  }

  double longitude_b = 0.0;
  double latitude_b = 0.0;
  if (!DecodeGeoMember(database_, args[1], args[3], longitude_b, latitude_b,
                       wrong_type)) {
    if (wrong_type) {
      return tl::make_unexpected(
          CommandError{.code = CommandErrorCode::kWrongType, .command = "geodist"});
    }
    return RespNullBulk{};
  }

  std::ostringstream stream;
  stream << std::fixed << std::setprecision(4)
         << GeoDistanceMeters(longitude_a, latitude_a, longitude_b, latitude_b);
  return RespBulkString{stream.str()};
}

CommandResult CommandProcessor::HandleGeosearch(
    const std::vector<std::string>& args) {
  if (args.size() != 8) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kWrongArity, .command = "geosearch"});
  }
  if (ToUpperAscii(args[2]) != "FROMLONLAT" ||
      ToUpperAscii(args[5]) != "BYRADIUS") {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kSyntaxError, .command = "geosearch"});
  }

  double center_longitude = 0.0;
  double center_latitude = 0.0;
  if (!ParseDouble(args[3], center_longitude) ||
      !ParseDouble(args[4], center_latitude)) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kSyntaxError, .command = "geosearch"});
  }

  double radius_meters = 0.0;
  if (!ParseGeoDistanceMeters(args[6], args[7], radius_meters)) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kSyntaxError, .command = "geosearch"});
  }

  const Database::ZEntriesResult entries = database_.ZEntries(args[1]);
  if (entries.wrong_type) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kWrongType, .command = "geosearch"});
  }

  std::vector<std::string> matches;
  for (const auto& [member, score_text] : entries.entries) {
    double score = 0.0;
    if (!ParseDouble(score_text, score) || score < 0.0) {
      continue;
    }

    const auto [member_longitude, member_latitude] =
        DecodeGeoScore(static_cast<uint64_t>(score));
    if (GeoDistanceMeters(center_longitude, center_latitude, member_longitude,
                          member_latitude) <= radius_meters) {
      matches.push_back(member);
    }
  }

  return RespArray{matches};
}

CommandResult CommandProcessor::HandleZadd(
    const std::vector<std::string>& args) {
  if (args.size() != 4) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "zadd"});
  }

  double score = 0.0;
  if (!ParseDouble(args[2], score)) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kSyntaxError, .command = "zadd"});
  }

  const Database::ZAddResult result =
      database_.ZAdd(args[1], score, args[2], args[3]);
  if (result.wrong_type) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongType, .command = "zadd"});
  }

  return RespInteger{result.added};
}

CommandResult CommandProcessor::HandleZrank(
    const std::vector<std::string>& args) {
  if (args.size() != 3) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "zrank"});
  }

  const Database::ZRankResult result = database_.ZRank(args[1], args[2]);
  if (result.wrong_type) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongType, .command = "zrank"});
  }
  if (!result.found) {
    return RespNullBulk{};
  }

  return RespInteger{result.rank};
}

CommandResult CommandProcessor::HandleZrange(
    const std::vector<std::string>& args) {
  if (args.size() != 4) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "zrange"});
  }

  int64_t start = 0;
  int64_t stop = 0;
  if (!ParseSignedInteger(args[2], start) ||
      !ParseSignedInteger(args[3], stop)) {
    return RespArray{};
  }

  const Database::ZRangeResult result = database_.ZRange(args[1], start, stop);
  if (result.wrong_type) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongType, .command = "zrange"});
  }

  return RespArray{result.members};
}

CommandResult CommandProcessor::HandleZcard(
    const std::vector<std::string>& args) {
  if (args.size() != 2) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "zcard"});
  }

  const Database::ZCardResult result = database_.ZCard(args[1]);
  if (result.wrong_type) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongType, .command = "zcard"});
  }

  return RespInteger{result.cardinality};
}

CommandResult CommandProcessor::HandleZscore(
    const std::vector<std::string>& args) {
  if (args.size() != 3) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "zscore"});
  }

  const Database::ZScoreResult result = database_.ZScore(args[1], args[2]);
  if (result.wrong_type) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongType, .command = "zscore"});
  }
  if (!result.found) {
    return RespNullBulk{};
  }

  return RespBulkString{result.score};
}

CommandResult CommandProcessor::HandleZrem(
    const std::vector<std::string>& args) {
  if (args.size() != 3) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "zrem"});
  }

  const Database::ZRemResult result = database_.ZRem(args[1], args[2]);
  if (result.wrong_type) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongType, .command = "zrem"});
  }

  return RespInteger{result.removed};
}

CommandResult CommandProcessor::HandleXadd(
    const std::vector<std::string>& args) {
  if (args.size() < 5 || args.size() % 2 == 0) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "xadd"});
  }

  std::vector<std::pair<std::string, std::string>> fields;
  fields.reserve((args.size() - 3) / 2);
  for (size_t index = 3; index < args.size(); index += 2) {
    fields.emplace_back(args[index], args[index + 1]);
  }

  const Database::StreamAddResult result =
      database_.XAdd(args[1], args[2], fields);
  switch (result.status) {
    case Database::StreamAddResult::Status::kOk:
      return RespBulkString{result.id};
    case Database::StreamAddResult::Status::kWrongType:
      return tl::make_unexpected(
          CommandError{.code = CommandErrorCode::kWrongType, .command = "xadd"});
    case Database::StreamAddResult::Status::kIdNotGreaterThanZeroZero:
      return tl::make_unexpected(CommandError{
          .code = CommandErrorCode::kXaddIdNotGreaterThanZeroZero,
          .command = "xadd"});
    case Database::StreamAddResult::Status::kIdNotGreaterThanTopItem:
      return tl::make_unexpected(CommandError{
          .code = CommandErrorCode::kXaddIdNotGreaterThanTopItem,
          .command = "xadd"});
    case Database::StreamAddResult::Status::kInvalidId:
      return tl::make_unexpected(
          CommandError{.code = CommandErrorCode::kSyntaxError, .command = "xadd"});
  }

  return tl::make_unexpected(
      CommandError{.code = CommandErrorCode::kSyntaxError, .command = "xadd"});
}

CommandResult CommandProcessor::HandleXrange(
    const std::vector<std::string>& args) {
  if (args.size() != 4) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kWrongArity, .command = "xrange"});
  }

  const Database::StreamRangeResult result =
      database_.XRange(args[1], args[2], args[3]);
  if (result.wrong_type) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kWrongType, .command = "xrange"});
  }
  if (result.invalid_id) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kSyntaxError, .command = "xrange"});
  }

  return RespRaw{EncodeStreamRange(result.entries)};
}

CommandResult CommandProcessor::HandleXread(
    const std::vector<std::string>& args) {
  if (args.size() < 4) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kWrongArity, .command = "xread"});
  }

  size_t streams_index = 1;
  std::optional<std::chrono::steady_clock::duration> block_timeout;
  if (ToUpperAscii(args[streams_index]) == "BLOCK") {
    if (args.size() < 6) {
      return tl::make_unexpected(CommandError{
          .code = CommandErrorCode::kWrongArity, .command = "xread"});
    }

    int64_t timeout_milliseconds = 0;
    if (!ParseMilliseconds(args[streams_index + 1], timeout_milliseconds)) {
      return tl::make_unexpected(CommandError{
          .code = CommandErrorCode::kInvalidInteger, .command = "xread"});
    }

    block_timeout = std::chrono::milliseconds(timeout_milliseconds);
    streams_index += 2;
  }

  if (streams_index >= args.size() ||
      ToUpperAscii(args[streams_index]) != "STREAMS" ||
      args.size() <= streams_index + 2 ||
      (args.size() - streams_index - 1) % 2 != 0) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kWrongArity, .command = "xread"});
  }

  const size_t stream_count = (args.size() - streams_index - 1) / 2;
  std::vector<std::pair<std::string, std::string>> stream_specs;
  stream_specs.reserve(stream_count);
  for (size_t index = 0; index < stream_count; ++index) {
    stream_specs.emplace_back(args[streams_index + 1 + index],
                              args[streams_index + 1 + stream_count + index]);
  }

  if (block_timeout.has_value()) {
    const Database::BlockingStreamReadResult result =
        database_.BlockingXRead(stream_specs, *block_timeout);
    if (result.wrong_type) {
      return tl::make_unexpected(CommandError{
          .code = CommandErrorCode::kWrongType, .command = "xread"});
    }
    if (result.invalid_id) {
      return tl::make_unexpected(CommandError{
          .code = CommandErrorCode::kSyntaxError, .command = "xread"});
    }
    if (result.streams.empty()) {
      return RespNullArray{};
    }

    return RespRaw{EncodeXreadResponse(result.streams)};
  }

  std::vector<std::pair<std::string, std::vector<Database::StreamRangeEntry>>>
      streams;
  streams.reserve(stream_count);

  for (size_t index = 0; index < stream_count; ++index) {
    const Database::StreamRangeResult result =
        database_.XRead(stream_specs[index].first, stream_specs[index].second);
    if (result.wrong_type) {
      return tl::make_unexpected(CommandError{
          .code = CommandErrorCode::kWrongType, .command = "xread"});
    }
    if (result.invalid_id) {
      return tl::make_unexpected(CommandError{
          .code = CommandErrorCode::kSyntaxError, .command = "xread"});
    }
    if (!result.entries.empty()) {
      streams.emplace_back(stream_specs[index].first, result.entries);
    }
  }

  if (streams.empty()) {
    return RespNullArray{};
  }

  return RespRaw{EncodeXreadResponse(streams)};
}

CommandResult CommandProcessor::HandleRpush(
    const std::vector<std::string>& args) {
  if (args.size() < 3) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kWrongArity, .command = "rpush"});
  }

  const std::vector<std::string> values(args.begin() + 2, args.end());
  const Database::ListMutationResult result =
      database_.PushRight(args[1], values);
  if (result.wrong_type) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongType, .command = "rpush"});
  }

  return RespInteger{result.size};
}

CommandResult CommandProcessor::HandleLpush(
    const std::vector<std::string>& args) {
  if (args.size() < 3) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kWrongArity, .command = "lpush"});
  }

  const std::vector<std::string> values(args.begin() + 2, args.end());
  const Database::ListMutationResult result =
      database_.PushLeft(args[1], values);
  if (result.wrong_type) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongType, .command = "lpush"});
  }

  return RespInteger{result.size};
}

CommandResult CommandProcessor::HandleLrange(
    const std::vector<std::string>& args) {
  if (args.size() < 4) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kWrongArity, .command = "lrange"});
  }

  int64_t start = 0;
  int64_t stop = 0;
  if (!ParseSignedInteger(args[2], start) ||
      !ParseSignedInteger(args[3], stop)) {
    return RespArray{};
  }

  const Database::ListRangeResult result =
      database_.Range(args[1], start, stop);
  if (result.wrong_type) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kWrongType, .command = "lrange"});
  }

  return RespArray{result.values};
}

CommandResult CommandProcessor::HandleLlen(
    const std::vector<std::string>& args) {
  if (args.size() < 2) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "llen"});
  }

  const Database::ListLengthResult result = database_.Length(args[1]);
  if (result.wrong_type) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongType, .command = "llen"});
  }

  return RespInteger{result.length};
}

CommandResult CommandProcessor::HandleLpop(
    const std::vector<std::string>& args) {
  if (args.size() < 2) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "lpop"});
  }

  if (args.size() >= 3) {
    int64_t count = 0;
    if (!ParseSignedInteger(args[2], count) || count <= 0) {
      return RespArray{};
    }

    const Database::ListPopManyResult result =
        database_.PopLeft(args[1], static_cast<size_t>(count));
    if (result.wrong_type) {
      return tl::make_unexpected(CommandError{
          .code = CommandErrorCode::kWrongType, .command = "lpop"});
    }

    return RespArray{result.values};
  }

  const Database::ListPopResult result = database_.PopLeft(args[1]);
  if (result.wrong_type) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongType, .command = "lpop"});
  }
  if (!result.found) {
    return RespNullBulk{};
  }

  return RespBulkString{result.value};
}

CommandResult CommandProcessor::HandleBlpop(
    const std::vector<std::string>& args) {
  if (args.size() < 3) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kWrongArity, .command = "blpop"});
  }

  std::chrono::steady_clock::duration timeout{};
  if (!ParseTimeoutDuration(args[2], timeout)) {
    return RespNullArray{};
  }

  const Database::BlockingPopResult result =
      database_.BlockingPopLeft(args[1], timeout);
  if (result.wrong_type) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongType, .command = "blpop"});
  }
  if (!result.found) {
    return RespNullArray{};
  }

  return RespArray{{result.key, result.value}};
}

CommandResult CommandProcessor::HandleIncr(
    const std::vector<std::string>& args) {
  if (args.size() != 2) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "incr"});
  }

  const Database::IncrResult result = database_.Incr(args[1]);
  if (result.wrong_type) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongType, .command = "incr"});
  }
  if (result.not_integer) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kInvalidInteger, .command = "incr"});
  }

  return RespInteger{result.value};
}

CommandResult CommandProcessor::HandleInfo(
    const std::vector<std::string>& args) {
  const std::string section =
      args.size() >= 2 ? ToUpperAscii(args[1]) : "ALL";
  if (section == "REPLICATION" || section == "ALL") {
    std::string info = "# Replication\r\n";
    info += std::string("role:") + (is_replica_ ? "slave" : "master") + "\r\n";
    info += "connected_slaves:0\r\n";
    info += "master_replid:" + std::string(kMasterReplId) + "\r\n";
    info += "master_repl_offset:0\r\n";
    return RespBulkString{std::move(info)};
  }

  return RespBulkString{""};
}

CommandResult CommandProcessor::HandleConfig(
    const std::vector<std::string>& args) {
  if (args.size() != 3 || ToUpperAscii(args[1]) != "GET") {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity,
                     .command = "config"});
  }

  const std::string parameter = ToUpperAscii(args[2]);
  if (server_config_ == nullptr) {
    return RespArray{{}};
  }
  if (parameter == "DIR") {
    return RespArray{{"dir", server_config_->dir}};
  }
  if (parameter == "DBFILENAME") {
    return RespArray{{"dbfilename", server_config_->dbfilename}};
  }

  return RespArray{{}};
}

CommandResult CommandProcessor::HandleReplconf(
    const std::vector<std::string>& args) {
  if (is_replica_ && args.size() == 3 &&
      ToUpperAscii(args[1]) == "GETACK" && args[2] == "*") {
    return RespArray{{"REPLCONF", "ACK", "0"}};
  }

  return RespSimpleString{"OK"};
}

CommandResult CommandProcessor::HandleWait(
    const std::vector<std::string>& args) {
  if (args.size() != 3) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "wait"});
  }

  int64_t num_replicas = 0;
  int64_t timeout_milliseconds = 0;
  if (!ParseMilliseconds(args[1], num_replicas) ||
      !ParseMilliseconds(args[2], timeout_milliseconds)) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kInvalidInteger,
                     .command = "wait"});
  }

  if (replica_manager_ == nullptr) {
    return RespInteger{0};
  }

  return RespInteger{replica_manager_->WaitForReplicas(
      static_cast<size_t>(num_replicas),
      std::chrono::milliseconds(timeout_milliseconds))};
}

}  // namespace redis
