#include "redis-cpp/database.hpp"

#include <algorithm>
#include <iostream>
#include <limits>

#include "redis-cpp/resp.hpp"

namespace redis {

std::string ValueTypeName(ValueType type) {
  switch (type) {
    case ValueType::kString:
      return "string";
    case ValueType::kList:
      return "list";
    case ValueType::kStream:
      return "stream";
    case ValueType::kSortedSet:
      return "zset";
    case ValueType::kNone:
    default:
      return "none";
  }
}

void Database::SetString(const std::string& key, std::string value,
                         std::optional<std::chrono::milliseconds> ttl) {
  Entry entry{
      .value = StringValue{.value = std::move(value)},
      .expires_at = std::nullopt,
  };
  if (ttl.has_value()) {
    entry.expires_at = std::chrono::steady_clock::now() + *ttl;
  }

  std::lock_guard<std::mutex> lock(mutex_);
  store_[key] = std::move(entry);
}

Database::StringLookup Database::GetString(const std::string& key) {
  std::lock_guard<std::mutex> lock(mutex_);
  Entry* entry = FindLiveEntryLocked(key);
  if (entry == nullptr) {
    return {};
  }

  if (!std::holds_alternative<StringValue>(entry->value)) {
    return {.type = EntryValueType(*entry), .value = std::nullopt};
  }

  return {
      .type = ValueType::kString,
      .value = std::get<StringValue>(entry->value).value,
  };
}

std::vector<std::string> Database::Keys() {
  std::lock_guard<std::mutex> lock(mutex_);

  const auto now = std::chrono::steady_clock::now();
  std::vector<std::string> keys;
  keys.reserve(store_.size());

  for (auto it = store_.begin(); it != store_.end();) {
    if (IsExpired(it->second, now)) {
      it = store_.erase(it);
      continue;
    }

    keys.push_back(it->first);
    ++it;
  }

  std::sort(keys.begin(), keys.end());
  return keys;
}

ValueType Database::TypeOf(const std::string& key) {
  std::lock_guard<std::mutex> lock(mutex_);
  Entry* entry = FindLiveEntryLocked(key);
  if (entry == nullptr) {
    return ValueType::kNone;
  }

  return EntryValueType(*entry);
}

Database::ListMutationResult Database::PushRight(
    const std::string& key, const std::vector<std::string>& values) {
  int64_t size = 0;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    Entry* entry = FindLiveEntryLocked(key);
    if (entry == nullptr) {
      auto [it, _] = store_.emplace(key, Entry{
                                             .value = ListValue{},
                                             .expires_at = std::nullopt,
                                         });
      entry = &it->second;
    }

    if (!std::holds_alternative<ListValue>(entry->value)) {
      return {.wrong_type = true};
    }

    entry->expires_at.reset();
    std::vector<std::string>& list = std::get<ListValue>(entry->value).values;
    list.insert(list.end(), values.begin(), values.end());
    size = static_cast<int64_t>(list.size());
  }

  list_change_cv_.notify_all();
  return {.size = size};
}

Database::ListMutationResult Database::PushLeft(
    const std::string& key, const std::vector<std::string>& values) {
  int64_t size = 0;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    Entry* entry = FindLiveEntryLocked(key);
    if (entry == nullptr) {
      auto [it, _] = store_.emplace(key, Entry{
                                             .value = ListValue{},
                                             .expires_at = std::nullopt,
                                         });
      entry = &it->second;
    }

    if (!std::holds_alternative<ListValue>(entry->value)) {
      return {.wrong_type = true};
    }

    entry->expires_at.reset();
    std::vector<std::string>& list = std::get<ListValue>(entry->value).values;
    for (const std::string& value : values) {
      list.insert(list.begin(), value);
    }
    size = static_cast<int64_t>(list.size());
  }

  list_change_cv_.notify_all();
  return {.size = size};
}

Database::ListRangeResult Database::Range(const std::string& key, int64_t start,
                                          int64_t stop) {
  std::lock_guard<std::mutex> lock(mutex_);
  Entry* entry = FindLiveEntryLocked(key);
  if (entry == nullptr) {
    return {};
  }

  if (!std::holds_alternative<ListValue>(entry->value)) {
    return {.wrong_type = true};
  }

  const std::vector<std::string>& list =
      std::get<ListValue>(entry->value).values;
  const int64_t list_size = static_cast<int64_t>(list.size());

  if (start < 0) {
    start += list_size;
  }
  if (stop < 0) {
    stop += list_size;
  }
  if (start < 0) {
    start = 0;
  }
  if (stop >= list_size) {
    stop = list_size - 1;
  }

  std::vector<std::string> values;
  if (start < list_size && stop >= 0 && start <= stop) {
    values.reserve(static_cast<size_t>(stop - start + 1));
    for (int64_t index = start; index <= stop; ++index) {
      values.push_back(list[static_cast<size_t>(index)]);
    }
  }

  return {.values = std::move(values)};
}

Database::ListLengthResult Database::Length(const std::string& key) {
  std::lock_guard<std::mutex> lock(mutex_);
  Entry* entry = FindLiveEntryLocked(key);
  if (entry == nullptr) {
    return {};
  }

  if (!std::holds_alternative<ListValue>(entry->value)) {
    return {.wrong_type = true};
  }

  return {
      .length =
          static_cast<int64_t>(std::get<ListValue>(entry->value).values.size()),
  };
}

Database::ListPopResult Database::PopLeft(const std::string& key) {
  std::lock_guard<std::mutex> lock(mutex_);
  Entry* entry = FindLiveEntryLocked(key);
  if (entry == nullptr) {
    return {};
  }

  if (!std::holds_alternative<ListValue>(entry->value)) {
    return {.wrong_type = true};
  }

  std::vector<std::string>& list = std::get<ListValue>(entry->value).values;
  if (list.empty()) {
    return {};
  }

  std::string value = list.front();
  list.erase(list.begin());
  return {
      .found = true,
      .value = std::move(value),
  };
}

Database::ListPopManyResult Database::PopLeft(const std::string& key,
                                              size_t count) {
  std::lock_guard<std::mutex> lock(mutex_);
  Entry* entry = FindLiveEntryLocked(key);
  if (entry == nullptr) {
    return {};
  }

  if (!std::holds_alternative<ListValue>(entry->value)) {
    return {.wrong_type = true};
  }

  std::vector<std::string>& list = std::get<ListValue>(entry->value).values;
  const size_t pop_count = std::min(count, list.size());

  std::vector<std::string> values;
  values.reserve(pop_count);
  for (size_t i = 0; i < pop_count; ++i) {
    values.push_back(list[i]);
  }
  list.erase(list.begin(),
             list.begin() + static_cast<std::ptrdiff_t>(pop_count));

  return {.values = std::move(values)};
}

Database::BlockingPopResult Database::BlockingPopLeft(
    const std::string& key, std::chrono::steady_clock::duration timeout) {
  std::unique_lock<std::mutex> lock(mutex_);

  auto list_ready = [&]() {
    Entry* entry = FindLiveEntryLocked(key);
    if (entry == nullptr) {
      return false;
    }

    if (!std::holds_alternative<ListValue>(entry->value)) {
      return true;
    }

    return !std::get<ListValue>(entry->value).values.empty();
  };

  Entry* entry = FindLiveEntryLocked(key);
  if (entry != nullptr && !std::holds_alternative<ListValue>(entry->value)) {
    return {.wrong_type = true};
  }

  if (timeout == std::chrono::steady_clock::duration::zero()) {
    list_change_cv_.wait(lock, list_ready);
  } else if (!list_change_cv_.wait_for(lock, timeout, list_ready)) {
    return {};
  }

  entry = FindLiveEntryLocked(key);
  if (entry == nullptr) {
    return {};
  }

  if (!std::holds_alternative<ListValue>(entry->value)) {
    return {.wrong_type = true};
  }

  std::vector<std::string>& list = std::get<ListValue>(entry->value).values;
  if (list.empty()) {
    return {};
  }

  std::string value = list.front();
  list.erase(list.begin());

  return {
      .found = true,
      .key = key,
      .value = std::move(value),
  };
}

Database::StreamAddResult Database::XAdd(
    const std::string& key, std::string id,
    const std::vector<std::pair<std::string, std::string>>& fields) {
  StreamAddResult result;
  {
    std::lock_guard<std::mutex> lock(mutex_);

    StreamId new_id;
    bool auto_generate_sequence = false;
    bool auto_generate_milliseconds = false;
    if (!ParseXAddStreamId(id, new_id, auto_generate_sequence,
                           auto_generate_milliseconds)) {
      return {.status = StreamAddResult::Status::kInvalidId};
    }

    Entry* entry = FindLiveEntryLocked(key);
    if (entry == nullptr) {
      auto [it, _] = store_.emplace(key, Entry{
                                             .value = StreamValue{},
                                             .expires_at = std::nullopt,
                                         });
      entry = &it->second;
    }

    if (!std::holds_alternative<StreamValue>(entry->value)) {
      return {.status = StreamAddResult::Status::kWrongType};
    }

    entry->expires_at.reset();
    std::vector<StreamEntry>& entries =
        std::get<StreamValue>(entry->value).entries;

    std::optional<StreamId> last_id;
    if (!entries.empty()) {
      StreamId parsed_last_id;
      if (!ParseStreamId(entries.back().id, parsed_last_id)) {
        return {.status = StreamAddResult::Status::kInvalidId};
      }
      last_id = parsed_last_id;
    }

    if (auto_generate_milliseconds) {
      const auto now = std::chrono::time_point_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now());
      new_id.milliseconds =
          static_cast<int64_t>(now.time_since_epoch().count());

      if (last_id.has_value() && new_id.milliseconds <= last_id->milliseconds) {
        new_id.milliseconds = last_id->milliseconds;
        new_id.sequence = last_id->sequence + 1;
      } else {
        new_id.sequence = 0;
      }
      id = FormatStreamId(new_id);
    } else if (auto_generate_sequence) {
      if (last_id.has_value() && last_id->milliseconds == new_id.milliseconds) {
        new_id.sequence = last_id->sequence + 1;
      } else {
        new_id.sequence = (new_id.milliseconds == 0) ? 1 : 0;
      }
      id = FormatStreamId(new_id);
    }

    if (CompareStreamIds(
            new_id, StreamId{.milliseconds = 0, .sequence = 0}) <= 0) {
      return {.status = StreamAddResult::Status::kIdNotGreaterThanZeroZero};
    }
    if (last_id.has_value() && CompareStreamIds(new_id, *last_id) <= 0) {
      return {.status = StreamAddResult::Status::kIdNotGreaterThanTopItem};
    }

    entries.push_back(StreamEntry{
        .id = std::move(id),
        .fields = fields,
    });

    result = {.status = StreamAddResult::Status::kOk, .id = entries.back().id};
  }

  std::cerr << "[XADD] added id=" << result.id << " key=" << key << " notify\n";
  stream_change_cv_.notify_all();
  return result;
}

Database::StreamRangeResult Database::XRange(const std::string& key,
                                             std::string_view start,
                                             std::string_view end) {
  std::lock_guard<std::mutex> lock(mutex_);

  StreamId start_id;
  StreamId end_id;
  if (!ParseStreamRangeId(start, true, start_id) ||
      !ParseStreamRangeId(end, false, end_id)) {
    return {.invalid_id = true};
  }

  Entry* entry = FindLiveEntryLocked(key);
  if (entry == nullptr) {
    return {};
  }

  if (!std::holds_alternative<StreamValue>(entry->value)) {
    return {.wrong_type = true};
  }

  const std::vector<StreamEntry>& stream_entries =
      std::get<StreamValue>(entry->value).entries;
  std::vector<StreamRangeEntry> entries;
  for (const StreamEntry& stream_entry : stream_entries) {
    StreamId current_id;
    if (!ParseStreamId(stream_entry.id, current_id)) {
      return {.invalid_id = true};
    }

    if (CompareStreamIds(current_id, start_id) < 0) {
      continue;
    }
    if (CompareStreamIds(current_id, end_id) > 0) {
      break;
    }

    StreamRangeEntry result_entry{.id = stream_entry.id};
    result_entry.values.reserve(stream_entry.fields.size() * 2);
    for (const auto& [field, value] : stream_entry.fields) {
      result_entry.values.push_back(field);
      result_entry.values.push_back(value);
    }
    entries.push_back(std::move(result_entry));
  }

  return {.entries = std::move(entries)};
}

Database::StreamRangeResult Database::XRead(const std::string& key,
                                            std::string_view start) {
  std::lock_guard<std::mutex> lock(mutex_);

  StreamId start_id;
  if (!ParseStreamId(start, start_id)) {
    return {.invalid_id = true};
  }

  Entry* entry = FindLiveEntryLocked(key);
  if (entry == nullptr) {
    return {};
  }

  if (!std::holds_alternative<StreamValue>(entry->value)) {
    return {.wrong_type = true};
  }

  const std::vector<StreamEntry>& stream_entries =
      std::get<StreamValue>(entry->value).entries;
  std::vector<StreamRangeEntry> entries;
  for (const StreamEntry& stream_entry : stream_entries) {
    StreamId current_id;
    if (!ParseStreamId(stream_entry.id, current_id)) {
      return {.invalid_id = true};
    }

    if (CompareStreamIds(current_id, start_id) <= 0) {
      continue;
    }

    StreamRangeEntry result_entry{.id = stream_entry.id};
    result_entry.values.reserve(stream_entry.fields.size() * 2);
    for (const auto& [field, value] : stream_entry.fields) {
      result_entry.values.push_back(field);
      result_entry.values.push_back(value);
    }
    entries.push_back(std::move(result_entry));
  }

  return {.entries = std::move(entries)};
}

Database::BlockingStreamReadResult Database::BlockingXRead(
    const std::vector<std::pair<std::string, std::string>>& streams,
    std::chrono::steady_clock::duration timeout) {
  std::unique_lock<std::mutex> lock(mutex_);

  // Resolve '$' to the current last ID of each stream before waiting.
  std::vector<std::pair<std::string, std::string>> resolved_streams;
  resolved_streams.reserve(streams.size());
  for (const auto& [key, start] : streams) {
    if (start == "$") {
      std::string last_id = "0-0";
      Entry* entry = FindLiveEntryLocked(key);
      if (entry != nullptr && std::holds_alternative<StreamValue>(entry->value)) {
        const std::vector<StreamEntry>& entries =
            std::get<StreamValue>(entry->value).entries;
        if (!entries.empty()) {
          last_id = entries.back().id;
        }
      }
      resolved_streams.emplace_back(key, std::move(last_id));
      std::cerr << "[XREAD_BLOCK] $ resolved to last_id=" << resolved_streams.back().second << " key=" << key << "\n";
    } else {
      resolved_streams.emplace_back(key, start);
    }
  }

  BlockingStreamReadResult result;
  auto read_streams = [&]() -> bool {
    result = {};
    result.streams.reserve(resolved_streams.size());

    for (const auto& [key, start] : resolved_streams) {
      StreamId start_id;
      if (!ParseStreamId(start, start_id)) {
        result.invalid_id = true;
        return true;
      }

      Entry* entry = FindLiveEntryLocked(key);
      if (entry == nullptr) {
        continue;
      }

      if (!std::holds_alternative<StreamValue>(entry->value)) {
        result.wrong_type = true;
        return true;
      }

      const std::vector<StreamEntry>& stream_entries =
          std::get<StreamValue>(entry->value).entries;
      std::vector<StreamRangeEntry> entries;
      for (const StreamEntry& stream_entry : stream_entries) {
        StreamId current_id;
        if (!ParseStreamId(stream_entry.id, current_id)) {
          result.invalid_id = true;
          return true;
        }

        if (CompareStreamIds(current_id, start_id) <= 0) {
          continue;
        }

        StreamRangeEntry result_entry{.id = stream_entry.id};
        result_entry.values.reserve(stream_entry.fields.size() * 2);
        for (const auto& [field, value] : stream_entry.fields) {
          result_entry.values.push_back(field);
          result_entry.values.push_back(value);
        }
        entries.push_back(std::move(result_entry));
      }

      if (!entries.empty()) {
        result.streams.emplace_back(key, std::move(entries));
      }
    }

    return result.wrong_type || result.invalid_id || !result.streams.empty();
  };

  if (read_streams()) {
    std::cerr << "[XREAD_BLOCK] immediate result (data already available)\n";
    return result;
  }

  std::cerr << "[XREAD_BLOCK] blocking, timeout=" << std::chrono::duration_cast<std::chrono::milliseconds>(timeout).count() << "ms\n";
  if (timeout == std::chrono::steady_clock::duration::zero()) {
    stream_change_cv_.wait(lock, read_streams);
    std::cerr << "[XREAD_BLOCK] woke up (infinite wait)\n";
    return result;
  }

  if (!stream_change_cv_.wait_for(lock, timeout, read_streams)) {
    std::cerr << "[XREAD_BLOCK] timed out\n";
    return {};
  }

  std::cerr << "[XREAD_BLOCK] woke up (timed wait)\n";
  return result;
}

Database::IncrResult Database::Incr(const std::string& key) {
  std::lock_guard<std::mutex> lock(mutex_);
  Entry* entry = FindLiveEntryLocked(key);

  int64_t current = 0;
  if (entry != nullptr) {
    if (!std::holds_alternative<StringValue>(entry->value)) {
      return {.wrong_type = true};
    }
    const std::string& str = std::get<StringValue>(entry->value).value;
    if (!ParseSignedInteger(str, current)) {
      return {.not_integer = true};
    }
  }

  const int64_t new_value = current + 1;
  const std::string new_str = std::to_string(new_value);

  if (entry == nullptr) {
    store_[key] = Entry{.value = StringValue{.value = new_str},
                        .expires_at = std::nullopt};
  } else {
    std::get<StringValue>(entry->value).value = new_str;
    entry->expires_at.reset();
  }

  return {.value = new_value};
}

Database::ZAddResult Database::ZAdd(const std::string& key, double score,
                                    const std::string& member) {
  std::lock_guard<std::mutex> lock(mutex_);
  Entry* entry = FindLiveEntryLocked(key);
  if (entry == nullptr) {
    auto [it, _] = store_.emplace(key, Entry{
                                           .value = SortedSetValue{},
                                           .expires_at = std::nullopt,
                                       });
    entry = &it->second;
  }

  if (!std::holds_alternative<SortedSetValue>(entry->value)) {
    return {.wrong_type = true};
  }

  entry->expires_at.reset();
  std::vector<SortedSetEntry>& entries =
      std::get<SortedSetValue>(entry->value).entries;
  for (SortedSetEntry& existing : entries) {
    if (existing.member != member) {
      continue;
    }

    existing.score = score;
    std::sort(entries.begin(), entries.end(),
              [](const SortedSetEntry& lhs, const SortedSetEntry& rhs) {
                if (lhs.score != rhs.score) {
                  return lhs.score < rhs.score;
                }
                return lhs.member < rhs.member;
              });
    return {.added = 0};
  }

  entries.push_back(SortedSetEntry{.member = member, .score = score});
  std::sort(entries.begin(), entries.end(),
            [](const SortedSetEntry& lhs, const SortedSetEntry& rhs) {
              if (lhs.score != rhs.score) {
                return lhs.score < rhs.score;
              }
              return lhs.member < rhs.member;
            });
  return {.added = 1};
}

Database::ZRankResult Database::ZRank(const std::string& key,
                                      const std::string& member) {
  std::lock_guard<std::mutex> lock(mutex_);
  Entry* entry = FindLiveEntryLocked(key);
  if (entry == nullptr) {
    return {};
  }

  if (!std::holds_alternative<SortedSetValue>(entry->value)) {
    return {.wrong_type = true};
  }

  const std::vector<SortedSetEntry>& entries =
      std::get<SortedSetValue>(entry->value).entries;
  for (size_t index = 0; index < entries.size(); ++index) {
    if (entries[index].member == member) {
      return {.found = true, .rank = static_cast<int64_t>(index)};
    }
  }

  return {};
}

Database::Entry* Database::FindLiveEntryLocked(const std::string& key) {
  const auto found = store_.find(key);
  if (found == store_.end()) {
    return nullptr;
  }

  const auto now = std::chrono::steady_clock::now();
  if (IsExpired(found->second, now)) {
    store_.erase(found);
    return nullptr;
  }

  return &found->second;
}

bool Database::ParseStreamId(std::string_view value, StreamId& id) {
  const size_t separator = value.find('-');
  if (separator == std::string_view::npos || separator == 0 ||
      separator == value.size() - 1) {
    return false;
  }

  return ParseMilliseconds(value.substr(0, separator), id.milliseconds) &&
         ParseMilliseconds(value.substr(separator + 1), id.sequence);
}

bool Database::ParseXAddStreamId(std::string_view value, StreamId& id,
                                 bool& auto_generate_sequence,
                                 bool& auto_generate_milliseconds) {
  auto_generate_sequence = false;
  auto_generate_milliseconds = false;
  if (value == "*") {
    id = {};
    auto_generate_sequence = true;
    auto_generate_milliseconds = true;
    return true;
  }

  if (ParseStreamId(value, id)) {
    return true;
  }

  const size_t separator = value.find('-');
  if (separator == std::string_view::npos || separator == 0 ||
      separator != value.size() - 2 || value.back() != '*') {
    return false;
  }

  if (!ParseMilliseconds(value.substr(0, separator), id.milliseconds)) {
    return false;
  }

  id.sequence = 0;
  auto_generate_sequence = true;
  return true;
}

bool Database::ParseStreamRangeId(std::string_view value, bool is_start,
                                  StreamId& id) {
  if (value == "-") {
    id = {};
    return true;
  }
  if (value == "+") {
    id.milliseconds = std::numeric_limits<int64_t>::max();
    id.sequence = std::numeric_limits<int64_t>::max();
    return true;
  }

  if (ParseStreamId(value, id)) {
    return true;
  }

  if (!ParseMilliseconds(value, id.milliseconds)) {
    return false;
  }

  id.sequence = is_start ? 0 : std::numeric_limits<int64_t>::max();
  return true;
}

std::string Database::FormatStreamId(const StreamId& id) {
  return std::to_string(id.milliseconds) + "-" + std::to_string(id.sequence);
}

int Database::CompareStreamIds(const StreamId& lhs, const StreamId& rhs) {
  if (lhs.milliseconds < rhs.milliseconds) {
    return -1;
  }
  if (lhs.milliseconds > rhs.milliseconds) {
    return 1;
  }
  if (lhs.sequence < rhs.sequence) {
    return -1;
  }
  if (lhs.sequence > rhs.sequence) {
    return 1;
  }
  return 0;
}

ValueType Database::EntryValueType(const Entry& entry) {
  if (std::holds_alternative<StringValue>(entry.value)) {
    return ValueType::kString;
  }
  if (std::holds_alternative<ListValue>(entry.value)) {
    return ValueType::kList;
  }
  if (std::holds_alternative<StreamValue>(entry.value)) {
    return ValueType::kStream;
  }
  return ValueType::kSortedSet;
}

bool Database::IsExpired(const Entry& entry,
                         std::chrono::steady_clock::time_point now) {
  return entry.expires_at.has_value() && now >= *entry.expires_at;
}

}  // namespace redis
