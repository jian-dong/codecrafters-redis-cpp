#pragma once

#include <chrono>
#include <cstdint>
#include <optional>
#include <string_view>

namespace redis {

[[nodiscard]] std::optional<int64_t> ParseSignedInteger(
    std::string_view text);
[[nodiscard]] std::optional<int64_t> ParseNonNegativeInteger(
    std::string_view text);
[[nodiscard]] std::optional<double> ParseFiniteDouble(std::string_view text);
[[nodiscard]] std::optional<std::chrono::steady_clock::duration>
ParseNonNegativeTimeout(std::string_view seconds_text);

}  // namespace redis
