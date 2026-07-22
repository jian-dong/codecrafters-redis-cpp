#pragma once

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include "tl/expected.hpp"

namespace redis {

enum class ValueType {
  kString,
  kList,
  kStream,
  kSortedSet,
};

std::string ValueTypeName(ValueType type);

enum class DatabaseErrorCode {
  kWrongType,
  kInvalidInteger,
  kIntegerOverflow,
  kInvalidStreamId,
  kStreamIdNotGreaterThanZeroZero,
  kStreamIdNotGreaterThanTopItem,
};

struct DatabaseError {
  DatabaseErrorCode code = DatabaseErrorCode::kWrongType;
  std::optional<ValueType> actual_type;
};

template <typename T> using DatabaseResult = tl::expected<T, DatabaseError>;

enum class WaitOutcome {
  kReady,
  kTimedOut,
};

class Database {
public:
  struct StreamRangeEntry {
    std::string id;
    std::vector<std::pair<std::string, std::string>> fields;
  };

  void SetString(const std::string &key, std::string value,
                 std::optional<std::chrono::milliseconds> ttl = std::nullopt);

  DatabaseResult<std::optional<std::string>> GetString(const std::string &key);
  std::vector<std::string> Keys();
  std::optional<ValueType> TypeOf(const std::string &key);

  DatabaseResult<int64_t> PushRight(const std::string &key,
                                    const std::vector<std::string> &values);
  DatabaseResult<int64_t> PushLeft(const std::string &key,
                                   const std::vector<std::string> &values);
  DatabaseResult<std::vector<std::string>> Range(const std::string &key,
                                                 int64_t start, int64_t stop);
  DatabaseResult<int64_t> Length(const std::string &key);
  DatabaseResult<std::optional<std::string>> PopLeft(const std::string &key);
  DatabaseResult<std::vector<std::string>> PopLeft(const std::string &key,
                                                   size_t count);
  WaitOutcome WaitForListReady(const std::string &key,
                               std::chrono::steady_clock::duration timeout);
  DatabaseResult<std::string>
  XAdd(const std::string &key, std::string id,
       const std::vector<std::pair<std::string, std::string>> &fields);
  DatabaseResult<std::vector<StreamRangeEntry>>
  XRange(const std::string &key, std::string_view start, std::string_view end);
  DatabaseResult<std::vector<StreamRangeEntry>> XRead(const std::string &key,
                                                      std::string_view start);
  DatabaseResult<std::optional<std::string>>
  LastStreamId(const std::string &key);
  uint64_t StreamGeneration();
  WaitOutcome WaitForStreamChange(uint64_t observed_generation,
                                  std::chrono::steady_clock::duration timeout);
  DatabaseResult<int64_t> Incr(const std::string &key);
  DatabaseResult<int64_t> ZAdd(const std::string &key, double score,
                               const std::string &score_text,
                               const std::string &member);
  DatabaseResult<std::optional<int64_t>> ZRank(const std::string &key,
                                               const std::string &member);
  DatabaseResult<std::vector<std::string>> ZRange(const std::string &key,
                                                  int64_t start, int64_t stop);
  DatabaseResult<int64_t> ZCard(const std::string &key);
  DatabaseResult<std::optional<std::string>> ZScore(const std::string &key,
                                                    const std::string &member);
  DatabaseResult<int64_t> ZRem(const std::string &key,
                               const std::string &member);
  DatabaseResult<std::vector<std::pair<std::string, std::string>>>
  ZEntries(const std::string &key);
  uint64_t KeyVersion(const std::string &key);

private:
  struct StringValue {
    std::string value;
  };

  struct ListValue {
    std::vector<std::string> values;
  };

  struct StreamEntry {
    std::string id;
    std::vector<std::pair<std::string, std::string>> fields;
  };

  struct StreamId {
    int64_t milliseconds = 0;
    int64_t sequence = 0;
  };

  enum class XAddIdGenerationPolicy {
    kNone,
    kSequence,
    kMillisecondsAndSequence,
  };

  struct ParsedXAddStreamId {
    StreamId id;
    XAddIdGenerationPolicy generation = XAddIdGenerationPolicy::kNone;
  };

  enum class StreamRangeBound {
    kStart,
    kEnd,
  };

  struct StreamValue {
    std::vector<StreamEntry> entries;
  };

  struct SortedSetEntry {
    std::string member;
    double score = 0.0;
    std::string score_text;
  };

  struct SortedSetValue {
    std::vector<SortedSetEntry> entries;
  };

  struct Entry {
    std::variant<StringValue, ListValue, StreamValue, SortedSetValue> value;
    std::optional<std::chrono::steady_clock::time_point> expires_at;
  };

  Entry *FindLiveEntryLocked(const std::string &key);
  static std::optional<StreamId> ParseStreamId(std::string_view value);
  static std::optional<ParsedXAddStreamId>
  ParseXAddStreamId(std::string_view value);
  static std::optional<StreamId>
  ParseStreamRangeId(std::string_view value, StreamRangeBound bound);
  static std::string FormatStreamId(const StreamId &id);
  static int CompareStreamIds(const StreamId &lhs, const StreamId &rhs);
  static ValueType EntryValueType(const Entry &entry);
  static bool IsExpired(const Entry &entry,
                        std::chrono::steady_clock::time_point now);

  std::unordered_map<std::string, Entry> store_;
  std::unordered_map<std::string, uint64_t> key_versions_;
  std::mutex mutex_;
  std::condition_variable list_change_cv_;
  std::condition_variable stream_change_cv_;
  uint64_t stream_generation_ = 0;
};

} // namespace redis
