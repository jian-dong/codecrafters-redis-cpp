#ifndef REDIS_CPP_DATABASE_HPP_
#define REDIS_CPP_DATABASE_HPP_

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
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
    bool wrong_type = false;
    std::string id;
  };

  void SetString(const std::string& key, std::string value,
                 std::optional<std::chrono::milliseconds> ttl = std::nullopt);

  StringLookup GetString(const std::string& key);
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

  struct StreamValue {
    std::vector<StreamEntry> entries;
  };

  struct Entry {
    std::variant<StringValue, ListValue, StreamValue> value;
    std::optional<std::chrono::steady_clock::time_point> expires_at;
  };

  Entry* FindLiveEntryLocked(const std::string& key);
  static ValueType EntryValueType(const Entry& entry);
  static bool IsExpired(const Entry& entry,
                        std::chrono::steady_clock::time_point now);

  std::unordered_map<std::string, Entry> store_;
  std::mutex mutex_;
  std::condition_variable list_change_cv_;
};

}  // namespace redis

#endif  // REDIS_CPP_DATABASE_HPP_
