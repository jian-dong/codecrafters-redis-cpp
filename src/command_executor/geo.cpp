#include "redis-cpp/command_executor.hpp"

#include <cmath>
#include <cstdint>
#include <iomanip>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "redis-cpp/numeric_parser.hpp"

namespace redis {
namespace {

constexpr double kMinLongitude = -180.0;
constexpr double kMaxLongitude = 180.0;
constexpr double kMinLatitude = -85.05112878;
constexpr double kMaxLatitude = 85.05112878;
constexpr int kGeoStepBits = 26;
constexpr uint64_t kMaxGeoScore = (1ULL << (kGeoStepBits * 2)) - 1;
constexpr double kEarthRadiusMeters = 6372797.560856;
constexpr double kDegreesToRadians = 3.14159265358979323846 / 180.0;
constexpr double kMetersPerKilometer = 1000.0;
constexpr double kMetersPerMile = 1609.344;
constexpr double kMetersPerFoot = 0.3048;

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

struct GeoPosition {
  double longitude = 0.0;
  double latitude = 0.0;
};

DatabaseResult<std::optional<GeoPosition>> DecodeGeoMember(
    Database& database, const std::string& key, const std::string& member) {
  const DatabaseResult<std::optional<std::string>> result =
      database.ZScore(key, member);
  if (!result) {
    return tl::make_unexpected(result.error());
  }
  if (!result->has_value()) {
    return std::optional<GeoPosition>{};
  }

  const std::optional<double> score = ParseFiniteDouble(**result);
  if (!score.has_value() || *score < 0.0 ||
      *score > static_cast<double>(kMaxGeoScore)) {
    return std::optional<GeoPosition>{};
  }

  const auto [longitude, latitude] =
      DecodeGeoScore(static_cast<uint64_t>(*score));
  return std::optional<GeoPosition>{GeoPosition{
      .longitude = longitude,
      .latitude = latitude,
  }};
}

std::optional<double> ParseGeoDistanceMeters(std::string_view radius_text,
                                             std::string_view unit_text) {
  const std::optional<double> radius = ParseFiniteDouble(radius_text);
  if (!radius.has_value()) {
    return std::nullopt;
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
    return std::nullopt;
  }

  return *radius * multiplier;
}

}  // namespace

CommandResult CommandExecutor::HandleGeoadd(
    const std::vector<std::string>& args) {
  if (args.size() != 5) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "geoadd"});
  }

  const std::optional<double> longitude = ParseFiniteDouble(args[2]);
  const std::optional<double> latitude = ParseFiniteDouble(args[3]);
  const bool longitude_valid =
      longitude.has_value() && *longitude >= kMinLongitude &&
      *longitude <= kMaxLongitude;
  const bool latitude_valid =
      latitude.has_value() && *latitude >= kMinLatitude &&
      *latitude <= kMaxLatitude;
  if (!longitude_valid || !latitude_valid) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kInvalidGeoCoordinates, .command = "geoadd"});
  }

  const uint64_t geo_score = EncodeGeoScore(*longitude, *latitude);
  const DatabaseResult<int64_t> result = database_.ZAdd(
      args[1], static_cast<double>(geo_score), std::to_string(geo_score),
      args[4]);
  if (!result) {
    return tl::make_unexpected(MapDatabaseError(result.error(), "geoadd"));
  }

  return RespInteger{*result};
}

CommandResult CommandExecutor::HandleGeopos(
    const std::vector<std::string>& args) {
  if (args.size() < 3) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "geopos"});
  }

  std::vector<RespValue> positions;
  positions.reserve(args.size() - 2);
  for (size_t index = 2; index < args.size(); ++index) {
    const DatabaseResult<std::optional<std::string>> result =
        database_.ZScore(args[1], args[index]);
    if (!result) {
      return tl::make_unexpected(
          MapDatabaseError(result.error(), "geopos"));
    }

    if (!result->has_value()) {
      positions.emplace_back(RespNullArray{});
      continue;
    }

    const std::optional<double> score = ParseFiniteDouble(**result);
    if (!score.has_value() || *score < 0.0 ||
        *score > static_cast<double>(kMaxGeoScore)) {
      positions.emplace_back(RespNullArray{});
      continue;
    }

    const auto [longitude, latitude] =
        DecodeGeoScore(static_cast<uint64_t>(*score));
    positions.emplace_back(RespArray::BulkStrings(
        {FormatGeoCoordinate(longitude), FormatGeoCoordinate(latitude)}));
  }

  return RespArray{std::move(positions)};
}

CommandResult CommandExecutor::HandleGeodist(
    const std::vector<std::string>& args) {
  if (args.size() != 4) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "geodist"});
  }

  const DatabaseResult<std::optional<GeoPosition>> first =
      DecodeGeoMember(database_, args[1], args[2]);
  if (!first) {
    return tl::make_unexpected(MapDatabaseError(first.error(), "geodist"));
  }
  if (!first->has_value()) {
    return RespNullBulk{};
  }

  const DatabaseResult<std::optional<GeoPosition>> second =
      DecodeGeoMember(database_, args[1], args[3]);
  if (!second) {
    return tl::make_unexpected(MapDatabaseError(second.error(), "geodist"));
  }
  if (!second->has_value()) {
    return RespNullBulk{};
  }

  std::ostringstream stream;
  stream << std::fixed << std::setprecision(4)
         << GeoDistanceMeters((*first)->longitude, (*first)->latitude,
                              (*second)->longitude, (*second)->latitude);
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

  const std::optional<double> center_longitude =
      ParseFiniteDouble(args[3]);
  const std::optional<double> center_latitude = ParseFiniteDouble(args[4]);
  if (!center_longitude.has_value() || !center_latitude.has_value()) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kSyntaxError, .command = "geosearch"});
  }

  const std::optional<double> radius_meters =
      ParseGeoDistanceMeters(args[6], args[7]);
  if (!radius_meters.has_value()) {
    return tl::make_unexpected(CommandError{
        .code = CommandErrorCode::kSyntaxError, .command = "geosearch"});
  }

  const DatabaseResult<
      std::vector<std::pair<std::string, std::string>>>
      entries = database_.ZEntries(args[1]);
  if (!entries) {
    return tl::make_unexpected(
        MapDatabaseError(entries.error(), "geosearch"));
  }

  std::vector<std::string> matches;
  for (const auto& [member, score_text] : *entries) {
    const std::optional<double> score = ParseFiniteDouble(score_text);
    if (!score.has_value() || *score < 0.0 ||
        *score > static_cast<double>(kMaxGeoScore)) {
      continue;
    }

    const auto [member_longitude, member_latitude] =
        DecodeGeoScore(static_cast<uint64_t>(*score));
    if (GeoDistanceMeters(*center_longitude, *center_latitude,
                          member_longitude, member_latitude) <=
        *radius_meters) {
      matches.push_back(member);
    }
  }

  return RespArray::BulkStrings(matches);
}

}  // namespace redis
