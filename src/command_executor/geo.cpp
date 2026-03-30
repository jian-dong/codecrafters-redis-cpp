#include "redis-cpp/command_executor.hpp"

#include <cerrno>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

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

}  // namespace

CommandResult CommandExecutor::HandleGeoadd(
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

CommandResult CommandExecutor::HandleGeopos(
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

CommandResult CommandExecutor::HandleGeodist(
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

CommandResult CommandExecutor::HandleGeosearch(
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

}  // namespace redis
