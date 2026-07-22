#include "redis-cpp/numeric_parser.hpp"

#include <cerrno>
#include <cctype>
#include <charconv>
#include <cmath>
#include <cstdlib>
#include <limits>
#include <string>
#include <type_traits>

namespace redis {

std::optional<int64_t> ParseSignedInteger(std::string_view text) {
  if (text.empty()) {
    return std::nullopt;
  }

  int64_t value = 0;
  const char* begin = text.data();
  const char* end = begin + text.size();
  const auto [next, error] = std::from_chars(begin, end, value);
  if (error != std::errc{} || next != end) {
    return std::nullopt;
  }
  return value;
}

std::optional<int64_t> ParseNonNegativeInteger(std::string_view text) {
  const std::optional<int64_t> value = ParseSignedInteger(text);
  if (!value.has_value() || *value < 0) {
    return std::nullopt;
  }
  return value;
}

std::optional<double> ParseFiniteDouble(std::string_view text) {
  if (text.empty() ||
      std::isspace(static_cast<unsigned char>(text.front())) != 0 ||
      std::isspace(static_cast<unsigned char>(text.back())) != 0) {
    return std::nullopt;
  }

  errno = 0;
  char* parse_end = nullptr;
  const std::string owned_text(text);
  const double value = std::strtod(owned_text.c_str(), &parse_end);
  if (parse_end != owned_text.c_str() + owned_text.size() || errno == ERANGE ||
      !std::isfinite(value)) {
    return std::nullopt;
  }
  return value;
}

std::optional<std::chrono::steady_clock::duration>
ParseNonNegativeTimeout(std::string_view seconds_text) {
  const std::optional<double> seconds = ParseFiniteDouble(seconds_text);
  if (!seconds.has_value() || *seconds < 0.0) {
    return std::nullopt;
  }

  using Duration = std::chrono::steady_clock::duration;
  using Rep = Duration::rep;
  static_assert(std::is_integral_v<Rep>);

  const long double ticks =
      static_cast<long double>(*seconds) *
      static_cast<long double>(Duration::period::den) /
      static_cast<long double>(Duration::period::num);
  if (!std::isfinite(ticks) ||
      ticks > static_cast<long double>(std::numeric_limits<Rep>::max())) {
    return std::nullopt;
  }

  return Duration{static_cast<Rep>(ticks)};
}

}  // namespace redis
