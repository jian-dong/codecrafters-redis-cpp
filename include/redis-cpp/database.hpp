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

namespace redis {

enum class ValueType {
  kNone,
  kString,
  kList,
  kStream,
  kSortedSet,
};

std::string ValueTypeName(ValueType type);

class Database {
 public:
  struct StringLookup {
    ValueType type = ValueType::kNone;
    std::optional<std::string> value;
  };

  struct ListMutationResult {
    bool wrong_type = false;
    int64_t size = 0;
  };

  struct ListRangeResult {
    bool wrong_type = false;
    std::vector<std::string> values;
  };

  struct ListLengthResult {
    bool wrong_type = false;
    int64_t length = 0;
  };

  struct ListPopResult {
    bool wrong_type = false;
    bool found = false;
    std::string value;
  };

  struct ListPopManyResult {
    bool wrong_type = false;
    std::vector<std::string> values;
  };

  struct BlockingPopResult {
    bool wrong_type = false;
    bool found = false;
    std::string key;
    std::string value;
  };

  struct StreamAddResult {
    enum class Status {
      kOk,
      kWrongType,
      kIdNotGreaterThanZeroZero,
      kIdNotGreaterThanTopItem,
      kInvalidId,
    };

    Status status = Status::kOk;
    std::string id;
  };

  struct StreamRangeEntry {
    std::string id;
    std::vector<std::string> values;
  };

  struct StreamRangeResult {
    bool wrong_type = false;
    bool invalid_id = false;
    std::vector<StreamRangeEntry> entries;
  };

  struct BlockingStreamReadResult {
    bool wrong_type = false;
    bool invalid_id = false;
    std::vector<std::pair<std::string, std::vector<StreamRangeEntry>>> streams;
  };

  struct IncrResult {
    bool wrong_type = false;
    bool not_integer = false;
    int64_t value = 0;
  };

  struct ZAddResult {
    bool wrong_type = false;
    int64_t added = 0;
  };

  struct ZRankResult {
    bool wrong_type = false;
    bool found = false;
    int64_t rank = 0;
  };

  struct ZRangeResult {
    bool wrong_type = false;
    std::vector<std::string> members;
  };

  void SetString(const std::string& key, std::string value,
                 std::optional<std::chrono::milliseconds> ttl = std::nullopt);

  StringLookup GetString(const std::string& key);
  std::vector<std::string> Keys();
  ValueType TypeOf(const std::string& key);

  ListMutationResult PushRight(const std::string& key,
                               const std::vector<std::string>& values);
  ListMutationResult PushLeft(const std::string& key,
                              const std::vector<std::string>& values);
  ListRangeResult Range(const std::string& key, int64_t start, int64_t stop);
  ListLengthResult Length(const std::string& key);
  ListPopResult PopLeft(const std::string& key);
  ListPopManyResult PopLeft(const std::string& key, size_t count);
  BlockingPopResult BlockingPopLeft(
      const std::string& key, std::chrono::steady_clock::duration timeout);
  StreamAddResult XAdd(
      const std::string& key, std::string id,
      const std::vector<std::pair<std::string, std::string>>& fields);
  StreamRangeResult XRange(const std::string& key, std::string_view start,
                           std::string_view end);
  StreamRangeResult XRead(const std::string& key, std::string_view start);
  BlockingStreamReadResult BlockingXRead(
      const std::vector<std::pair<std::string, std::string>>& streams,
      std::chrono::steady_clock::duration timeout);
  IncrResult Incr(const std::string& key);
  ZAddResult ZAdd(const std::string& key, double score,
                  const std::string& member);
  ZRankResult ZRank(const std::string& key, const std::string& member);
  ZRangeResult ZRange(const std::string& key, int64_t start, int64_t stop);

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

  struct StreamValue {
    std::vector<StreamEntry> entries;
  };

  struct SortedSetEntry {
    std::string member;
    double score = 0.0;
  };

  struct SortedSetValue {
    std::vector<SortedSetEntry> entries;
  };

  struct Entry {
    std::variant<StringValue, ListValue, StreamValue, SortedSetValue> value;
    std::optional<std::chrono::steady_clock::time_point> expires_at;
  };

  Entry* FindLiveEntryLocked(const std::string& key);
  static bool ParseStreamId(std::string_view value, StreamId& id);
  static bool ParseXAddStreamId(std::string_view value, StreamId& id,
                                bool& auto_generate_sequence,
                                bool& auto_generate_milliseconds);
  static bool ParseStreamRangeId(std::string_view value, bool is_start,
                                 StreamId& id);
  static std::string FormatStreamId(const StreamId& id);
  static int CompareStreamIds(const StreamId& lhs, const StreamId& rhs);
  static ValueType EntryValueType(const Entry& entry);
  static bool IsExpired(const Entry& entry,
                        std::chrono::steady_clock::time_point now);

  std::unordered_map<std::string, Entry> store_;
  std::mutex mutex_;
  std::condition_variable list_change_cv_;
  std::condition_variable stream_change_cv_;
};

}  // namespace redis
