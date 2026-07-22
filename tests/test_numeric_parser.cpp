#include <chrono>
#include <cstdint>
#include <limits>
#include <string>

#include <gtest/gtest.h>

#include "redis-cpp/numeric_parser.hpp"

namespace {

TEST(NumericParserTest, ParsesSignedIntegerAcrossItsFullRange) {
  const auto zero = redis::ParseSignedInteger("0");
  const auto negative = redis::ParseSignedInteger("-42");
  const auto minimum = redis::ParseSignedInteger(
      std::to_string(std::numeric_limits<int64_t>::min()));
  const auto maximum = redis::ParseSignedInteger(
      std::to_string(std::numeric_limits<int64_t>::max()));

  ASSERT_TRUE(zero.has_value());
  EXPECT_EQ(*zero, 0);
  ASSERT_TRUE(negative.has_value());
  EXPECT_EQ(*negative, -42);
  ASSERT_TRUE(minimum.has_value());
  EXPECT_EQ(*minimum, std::numeric_limits<int64_t>::min());
  ASSERT_TRUE(maximum.has_value());
  EXPECT_EQ(*maximum, std::numeric_limits<int64_t>::max());
}

TEST(NumericParserTest, RejectsMalformedAndOutOfRangeIntegers) {
  EXPECT_FALSE(redis::ParseSignedInteger("").has_value());
  EXPECT_FALSE(redis::ParseSignedInteger("+1").has_value());
  EXPECT_FALSE(redis::ParseSignedInteger(" 1").has_value());
  EXPECT_FALSE(redis::ParseSignedInteger("1 ").has_value());
  EXPECT_FALSE(redis::ParseSignedInteger("1tail").has_value());
  EXPECT_FALSE(redis::ParseSignedInteger("9223372036854775808").has_value());
  EXPECT_FALSE(redis::ParseSignedInteger("-9223372036854775809").has_value());
}

TEST(NumericParserTest, ParsesOnlyNonNegativeIntegers) {
  const auto zero = redis::ParseNonNegativeInteger("0");
  const auto maximum = redis::ParseNonNegativeInteger(
      std::to_string(std::numeric_limits<int64_t>::max()));

  ASSERT_TRUE(zero.has_value());
  EXPECT_EQ(*zero, 0);
  ASSERT_TRUE(maximum.has_value());
  EXPECT_EQ(*maximum, std::numeric_limits<int64_t>::max());
  EXPECT_FALSE(redis::ParseNonNegativeInteger("-1").has_value());
  EXPECT_FALSE(redis::ParseNonNegativeInteger("12x").has_value());
}

TEST(NumericParserTest, ParsesFiniteFloatingPointValues) {
  const auto decimal = redis::ParseFiniteDouble("-1.25");
  const auto exponent = redis::ParseFiniteDouble("2.5e3");

  ASSERT_TRUE(decimal.has_value());
  EXPECT_DOUBLE_EQ(*decimal, -1.25);
  ASSERT_TRUE(exponent.has_value());
  EXPECT_DOUBLE_EQ(*exponent, 2500.0);
}

TEST(NumericParserTest, RejectsMalformedAndNonFiniteFloatingPointValues) {
  EXPECT_FALSE(redis::ParseFiniteDouble("").has_value());
  EXPECT_FALSE(redis::ParseFiniteDouble(" 1.5").has_value());
  EXPECT_FALSE(redis::ParseFiniteDouble("1.5 ").has_value());
  EXPECT_FALSE(redis::ParseFiniteDouble("1.5tail").has_value());
  EXPECT_FALSE(redis::ParseFiniteDouble("NaN").has_value());
  EXPECT_FALSE(redis::ParseFiniteDouble("Infinity").has_value());
  EXPECT_FALSE(redis::ParseFiniteDouble("-inf").has_value());
  EXPECT_FALSE(redis::ParseFiniteDouble("1e9999").has_value());
  EXPECT_FALSE(redis::ParseFiniteDouble("1e-9999").has_value());
}

TEST(NumericParserTest, ParsesNonNegativeTimeoutsIntoSteadyClockDurations) {
  using namespace std::chrono_literals;

  const auto zero = redis::ParseNonNegativeTimeout("0");
  const auto fractional = redis::ParseNonNegativeTimeout("1.5");

  ASSERT_TRUE(zero.has_value());
  EXPECT_EQ(*zero, std::chrono::steady_clock::duration::zero());
  ASSERT_TRUE(fractional.has_value());
  EXPECT_EQ(*fractional,
            std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                1500ms));
}

TEST(NumericParserTest, RejectsInvalidOrOutOfRangeTimeouts) {
  EXPECT_FALSE(redis::ParseNonNegativeTimeout("-0.01").has_value());
  EXPECT_FALSE(redis::ParseNonNegativeTimeout("NaN").has_value());
  EXPECT_FALSE(redis::ParseNonNegativeTimeout("Infinity").has_value());
  EXPECT_FALSE(redis::ParseNonNegativeTimeout("1 second").has_value());
  EXPECT_FALSE(redis::ParseNonNegativeTimeout("1e100").has_value());
}

}  // namespace
