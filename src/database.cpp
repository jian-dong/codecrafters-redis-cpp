#include "redis-cpp/database.hpp"

#include <algorithm>
#include <limits>

#include "redis-cpp/numeric_parser.hpp"

namespace redis {
namespace {

std::chrono::steady_clock::time_point ExpirationDeadline(
    std::chrono::milliseconds ttl) {
  using Clock = std::chrono::steady_clock;

  const Clock::time_point now = Clock::now();
  if (ttl <= std::chrono::milliseconds::zero()) {
    return now;
  }

  const std::chrono::milliseconds remaining =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          Clock::time_point::max() - now);
  if (ttl > remaining) {
    return Clock::time_point::max();
  }
  return now + ttl;
}

}  // namespace

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
  }
  return "none";
}

void Database::SetString(const std::string &key, std::string value,
                         std::optional<std::chrono::milliseconds> ttl) {
  Entry entry{
      .value = StringValue{.value = std::move(value)},
      .expires_at = std::nullopt,
  };
  if (ttl.has_value()) {
    entry.expires_at = ExpirationDeadline(*ttl);
  }

  std::lock_guard<std::mutex> lock(mutex_);
  store_[key] = std::move(entry);
  ++key_versions_[key];
}

DatabaseResult<std::optional<std::string>>
Database::GetString(const std::string &key) {
  std::lock_guard<std::mutex> lock(mutex_);
  Entry *entry = FindLiveEntryLocked(key);
  if (entry == nullptr) {
    return std::optional<std::string>{};
  }

  if (!std::holds_alternative<StringValue>(entry->value)) {
    return tl::make_unexpected(DatabaseError{
        .code = DatabaseErrorCode::kWrongType,
        .actual_type = EntryValueType(*entry),
    });
  }

  return std::optional<std::string>{std::get<StringValue>(entry->value).value};
}

std::vector<std::string> Database::Keys() {
  std::lock_guard<std::mutex> lock(mutex_);

  const auto now = std::chrono::steady_clock::now();
  std::vector<std::string> keys;
  keys.reserve(store_.size());

  for (auto it = store_.begin(); it != store_.end();) {
    if (IsExpired(it->second, now)) {
      ++key_versions_[it->first];
      it = store_.erase(it);
      continue;
    }

    keys.push_back(it->first);
    ++it;
  }

  std::sort(keys.begin(), keys.end());
  return keys;
}

std::optional<ValueType> Database::TypeOf(const std::string &key) {
  std::lock_guard<std::mutex> lock(mutex_);
  Entry *entry = FindLiveEntryLocked(key);
  if (entry == nullptr) {
    return std::nullopt;
  }

  return EntryValueType(*entry);
}

DatabaseResult<int64_t>
Database::PushRight(const std::string &key,
                    const std::vector<std::string> &values) {
  int64_t size = 0;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    Entry *entry = FindLiveEntryLocked(key);
    if (entry == nullptr) {
      auto [it, _] = store_.emplace(key, Entry{
                                             .value = ListValue{},
                                             .expires_at = std::nullopt,
                                         });
      entry = &it->second;
    }

    if (!std::holds_alternative<ListValue>(entry->value)) {
      return tl::make_unexpected(DatabaseError{
          .code = DatabaseErrorCode::kWrongType,
          .actual_type = EntryValueType(*entry),
      });
    }

    entry->expires_at.reset();
    std::vector<std::string> &list = std::get<ListValue>(entry->value).values;
    list.insert(list.end(), values.begin(), values.end());
    size = static_cast<int64_t>(list.size());
    ++key_versions_[key];
  }

  list_change_cv_.notify_all();
  return size;
}

DatabaseResult<int64_t>
Database::PushLeft(const std::string &key,
                   const std::vector<std::string> &values) {
  int64_t size = 0;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    Entry *entry = FindLiveEntryLocked(key);
    if (entry == nullptr) {
      auto [it, _] = store_.emplace(key, Entry{
                                             .value = ListValue{},
                                             .expires_at = std::nullopt,
                                         });
      entry = &it->second;
    }

    if (!std::holds_alternative<ListValue>(entry->value)) {
      return tl::make_unexpected(DatabaseError{
          .code = DatabaseErrorCode::kWrongType,
          .actual_type = EntryValueType(*entry),
      });
    }

    entry->expires_at.reset();
    std::vector<std::string> &list = std::get<ListValue>(entry->value).values;
    for (const std::string &value : values) {
      list.insert(list.begin(), value);
    }
    size = static_cast<int64_t>(list.size());
    ++key_versions_[key];
  }

  list_change_cv_.notify_all();
  return size;
}

DatabaseResult<std::vector<std::string>>
Database::Range(const std::string &key, int64_t start, int64_t stop) {
  std::lock_guard<std::mutex> lock(mutex_);
  Entry *entry = FindLiveEntryLocked(key);
  if (entry == nullptr) {
    return std::vector<std::string>{};
  }

  if (!std::holds_alternative<ListValue>(entry->value)) {
    return tl::make_unexpected(DatabaseError{
        .code = DatabaseErrorCode::kWrongType,
        .actual_type = EntryValueType(*entry),
    });
  }

  const std::vector<std::string> &list =
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

  return values;
}

DatabaseResult<int64_t> Database::Length(const std::string &key) {
  std::lock_guard<std::mutex> lock(mutex_);
  Entry *entry = FindLiveEntryLocked(key);
  if (entry == nullptr) {
    return int64_t{0};
  }

  if (!std::holds_alternative<ListValue>(entry->value)) {
    return tl::make_unexpected(DatabaseError{
        .code = DatabaseErrorCode::kWrongType,
        .actual_type = EntryValueType(*entry),
    });
  }

  return static_cast<int64_t>(std::get<ListValue>(entry->value).values.size());
}

DatabaseResult<std::optional<std::string>>
Database::PopLeft(const std::string &key) {
  std::lock_guard<std::mutex> lock(mutex_);
  Entry *entry = FindLiveEntryLocked(key);
  if (entry == nullptr) {
    return std::optional<std::string>{};
  }

  if (!std::holds_alternative<ListValue>(entry->value)) {
    return tl::make_unexpected(DatabaseError{
        .code = DatabaseErrorCode::kWrongType,
        .actual_type = EntryValueType(*entry),
    });
  }

  std::vector<std::string> &list = std::get<ListValue>(entry->value).values;
  if (list.empty()) {
    return std::optional<std::string>{};
  }

  std::string value = list.front();
  list.erase(list.begin());
  ++key_versions_[key];
  return std::optional<std::string>{std::move(value)};
}

DatabaseResult<std::vector<std::string>>
Database::PopLeft(const std::string &key, size_t count) {
  std::lock_guard<std::mutex> lock(mutex_);
  Entry *entry = FindLiveEntryLocked(key);
  if (entry == nullptr) {
    return std::vector<std::string>{};
  }

  if (!std::holds_alternative<ListValue>(entry->value)) {
    return tl::make_unexpected(DatabaseError{
        .code = DatabaseErrorCode::kWrongType,
        .actual_type = EntryValueType(*entry),
    });
  }

  std::vector<std::string> &list = std::get<ListValue>(entry->value).values;
  const size_t pop_count = std::min(count, list.size());

  std::vector<std::string> values;
  values.reserve(pop_count);
  for (size_t i = 0; i < pop_count; ++i) {
    values.push_back(list[i]);
  }
  list.erase(list.begin(),
             list.begin() + static_cast<std::ptrdiff_t>(pop_count));
  if (pop_count > 0) {
    ++key_versions_[key];
  }

  return values;
}

WaitOutcome
Database::WaitForListReady(const std::string &key,
                           std::chrono::steady_clock::duration timeout) {
  std::unique_lock<std::mutex> lock(mutex_);
  auto ready = [&]() {
    Entry *entry = FindLiveEntryLocked(key);
    return entry != nullptr &&
           (!std::holds_alternative<ListValue>(entry->value) ||
            !std::get<ListValue>(entry->value).values.empty());
  };

  if (timeout == std::chrono::steady_clock::duration::zero()) {
    list_change_cv_.wait(lock, ready);
    return WaitOutcome::kReady;
  }
  return list_change_cv_.wait_for(lock, timeout, ready)
             ? WaitOutcome::kReady
             : WaitOutcome::kTimedOut;
}

DatabaseResult<std::string>
Database::XAdd(const std::string &key, std::string id,
               const std::vector<std::pair<std::string, std::string>> &fields) {
  std::string stored_id;
  {
    std::lock_guard<std::mutex> lock(mutex_);

    const std::optional<ParsedXAddStreamId> parsed_id =
        ParseXAddStreamId(id);
    if (!parsed_id.has_value()) {
      return tl::make_unexpected(
          DatabaseError{.code = DatabaseErrorCode::kInvalidStreamId});
    }
    StreamId new_id = parsed_id->id;

    Entry *entry = FindLiveEntryLocked(key);
    if (entry != nullptr &&
        !std::holds_alternative<StreamValue>(entry->value)) {
      return tl::make_unexpected(DatabaseError{
          .code = DatabaseErrorCode::kWrongType,
          .actual_type = EntryValueType(*entry),
      });
    }

    std::optional<StreamId> last_id;
    if (entry != nullptr &&
        !std::get<StreamValue>(entry->value).entries.empty()) {
      const std::optional<StreamId> parsed_last_id = ParseStreamId(
          std::get<StreamValue>(entry->value).entries.back().id);
      if (!parsed_last_id.has_value()) {
        return tl::make_unexpected(
            DatabaseError{.code = DatabaseErrorCode::kInvalidStreamId});
      }
      last_id = *parsed_last_id;
    }

    if (parsed_id->generation ==
        XAddIdGenerationPolicy::kMillisecondsAndSequence) {
      const auto now = std::chrono::time_point_cast<std::chrono::milliseconds>(
          std::chrono::system_clock::now());
      new_id.milliseconds =
          static_cast<int64_t>(now.time_since_epoch().count());

      if (last_id.has_value() && new_id.milliseconds <= last_id->milliseconds) {
        new_id.milliseconds = last_id->milliseconds;
        if (last_id->sequence == std::numeric_limits<int64_t>::max()) {
          return tl::make_unexpected(
              DatabaseError{.code = DatabaseErrorCode::kInvalidStreamId});
        }
        new_id.sequence = last_id->sequence + 1;
      } else {
        new_id.sequence = 0;
      }
      id = FormatStreamId(new_id);
    } else if (parsed_id->generation == XAddIdGenerationPolicy::kSequence) {
      if (last_id.has_value() && last_id->milliseconds == new_id.milliseconds) {
        if (last_id->sequence == std::numeric_limits<int64_t>::max()) {
          return tl::make_unexpected(
              DatabaseError{.code = DatabaseErrorCode::kInvalidStreamId});
        }
        new_id.sequence = last_id->sequence + 1;
      } else {
        new_id.sequence = (new_id.milliseconds == 0) ? 1 : 0;
      }
      id = FormatStreamId(new_id);
    }

    if (CompareStreamIds(new_id, StreamId{.milliseconds = 0, .sequence = 0}) <=
        0) {
      return tl::make_unexpected(DatabaseError{
          .code = DatabaseErrorCode::kStreamIdNotGreaterThanZeroZero});
    }
    if (last_id.has_value() && CompareStreamIds(new_id, *last_id) <= 0) {
      return tl::make_unexpected(DatabaseError{
          .code = DatabaseErrorCode::kStreamIdNotGreaterThanTopItem});
    }

    if (entry == nullptr) {
      auto [it, _] = store_.emplace(key, Entry{
                                             .value = StreamValue{},
                                             .expires_at = std::nullopt,
                                         });
      entry = &it->second;
    }
    entry->expires_at.reset();
    std::vector<StreamEntry> &entries =
        std::get<StreamValue>(entry->value).entries;

    entries.push_back(StreamEntry{
        .id = std::move(id),
        .fields = fields,
    });
    ++key_versions_[key];
    ++stream_generation_;

    stored_id = entries.back().id;
  }

  stream_change_cv_.notify_all();
  return stored_id;
}

DatabaseResult<std::vector<Database::StreamRangeEntry>>
Database::XRange(const std::string &key, std::string_view start,
                 std::string_view end) {
  std::lock_guard<std::mutex> lock(mutex_);

  const std::optional<StreamId> start_id =
      ParseStreamRangeId(start, StreamRangeBound::kStart);
  const std::optional<StreamId> end_id =
      ParseStreamRangeId(end, StreamRangeBound::kEnd);
  if (!start_id.has_value() || !end_id.has_value()) {
    return tl::make_unexpected(
        DatabaseError{.code = DatabaseErrorCode::kInvalidStreamId});
  }

  Entry *entry = FindLiveEntryLocked(key);
  if (entry == nullptr) {
    return std::vector<StreamRangeEntry>{};
  }

  if (!std::holds_alternative<StreamValue>(entry->value)) {
    return tl::make_unexpected(DatabaseError{
        .code = DatabaseErrorCode::kWrongType,
        .actual_type = EntryValueType(*entry),
    });
  }

  const std::vector<StreamEntry> &stream_entries =
      std::get<StreamValue>(entry->value).entries;
  std::vector<StreamRangeEntry> entries;
  for (const StreamEntry &stream_entry : stream_entries) {
    const std::optional<StreamId> current_id = ParseStreamId(stream_entry.id);
    if (!current_id.has_value()) {
      return tl::make_unexpected(
          DatabaseError{.code = DatabaseErrorCode::kInvalidStreamId});
    }

    if (CompareStreamIds(*current_id, *start_id) < 0) {
      continue;
    }
    if (CompareStreamIds(*current_id, *end_id) > 0) {
      break;
    }

    entries.push_back(StreamRangeEntry{
        .id = stream_entry.id,
        .fields = stream_entry.fields,
    });
  }

  return entries;
}

DatabaseResult<std::vector<Database::StreamRangeEntry>>
Database::XRead(const std::string &key, std::string_view start) {
  std::lock_guard<std::mutex> lock(mutex_);

  const std::optional<StreamId> start_id = ParseStreamId(start);
  if (!start_id.has_value()) {
    return tl::make_unexpected(
        DatabaseError{.code = DatabaseErrorCode::kInvalidStreamId});
  }

  Entry *entry = FindLiveEntryLocked(key);
  if (entry == nullptr) {
    return std::vector<StreamRangeEntry>{};
  }

  if (!std::holds_alternative<StreamValue>(entry->value)) {
    return tl::make_unexpected(DatabaseError{
        .code = DatabaseErrorCode::kWrongType,
        .actual_type = EntryValueType(*entry),
    });
  }

  const std::vector<StreamEntry> &stream_entries =
      std::get<StreamValue>(entry->value).entries;
  std::vector<StreamRangeEntry> entries;
  for (const StreamEntry &stream_entry : stream_entries) {
    const std::optional<StreamId> current_id = ParseStreamId(stream_entry.id);
    if (!current_id.has_value()) {
      return tl::make_unexpected(
          DatabaseError{.code = DatabaseErrorCode::kInvalidStreamId});
    }

    if (CompareStreamIds(*current_id, *start_id) <= 0) {
      continue;
    }

    entries.push_back(StreamRangeEntry{
        .id = stream_entry.id,
        .fields = stream_entry.fields,
    });
  }

  return entries;
}

DatabaseResult<std::optional<std::string>>
Database::LastStreamId(const std::string &key) {
  std::lock_guard<std::mutex> lock(mutex_);
  Entry *entry = FindLiveEntryLocked(key);
  if (entry == nullptr) {
    return std::optional<std::string>{};
  }
  if (!std::holds_alternative<StreamValue>(entry->value)) {
    return tl::make_unexpected(DatabaseError{
        .code = DatabaseErrorCode::kWrongType,
        .actual_type = EntryValueType(*entry),
    });
  }

  const auto &entries = std::get<StreamValue>(entry->value).entries;
  if (entries.empty()) {
    return std::optional<std::string>{};
  }
  return std::optional<std::string>{entries.back().id};
}

uint64_t Database::StreamGeneration() {
  std::lock_guard<std::mutex> lock(mutex_);
  return stream_generation_;
}

WaitOutcome
Database::WaitForStreamChange(uint64_t observed_generation,
                              std::chrono::steady_clock::duration timeout) {
  std::unique_lock<std::mutex> lock(mutex_);
  auto changed = [&] { return stream_generation_ != observed_generation; };
  if (timeout == std::chrono::steady_clock::duration::zero()) {
    stream_change_cv_.wait(lock, changed);
    return WaitOutcome::kReady;
  }
  return stream_change_cv_.wait_for(lock, timeout, changed)
             ? WaitOutcome::kReady
             : WaitOutcome::kTimedOut;
}

DatabaseResult<int64_t> Database::Incr(const std::string &key) {
  std::lock_guard<std::mutex> lock(mutex_);
  Entry *entry = FindLiveEntryLocked(key);

  int64_t current = 0;
  if (entry != nullptr) {
    if (!std::holds_alternative<StringValue>(entry->value)) {
      return tl::make_unexpected(DatabaseError{
          .code = DatabaseErrorCode::kWrongType,
          .actual_type = EntryValueType(*entry),
      });
    }
    const std::string &str = std::get<StringValue>(entry->value).value;
    const std::optional<int64_t> parsed = ParseSignedInteger(str);
    if (!parsed.has_value()) {
      return tl::make_unexpected(
          DatabaseError{.code = DatabaseErrorCode::kInvalidInteger});
    }
    current = *parsed;
  }

  if (current == std::numeric_limits<int64_t>::max()) {
    return tl::make_unexpected(
        DatabaseError{.code = DatabaseErrorCode::kIntegerOverflow});
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
  ++key_versions_[key];

  return new_value;
}

DatabaseResult<int64_t> Database::ZAdd(const std::string &key, double score,
                                       const std::string &score_text,
                                       const std::string &member) {
  std::lock_guard<std::mutex> lock(mutex_);
  Entry *entry = FindLiveEntryLocked(key);
  if (entry == nullptr) {
    auto [it, _] = store_.emplace(key, Entry{
                                           .value = SortedSetValue{},
                                           .expires_at = std::nullopt,
                                       });
    entry = &it->second;
  }

  if (!std::holds_alternative<SortedSetValue>(entry->value)) {
    return tl::make_unexpected(DatabaseError{
        .code = DatabaseErrorCode::kWrongType,
        .actual_type = EntryValueType(*entry),
    });
  }

  entry->expires_at.reset();
  std::vector<SortedSetEntry> &entries =
      std::get<SortedSetValue>(entry->value).entries;
  for (SortedSetEntry &existing : entries) {
    if (existing.member != member) {
      continue;
    }

    existing.score = score;
    existing.score_text = score_text;
    std::sort(entries.begin(), entries.end(),
              [](const SortedSetEntry &lhs, const SortedSetEntry &rhs) {
                if (lhs.score != rhs.score) {
                  return lhs.score < rhs.score;
                }
                return lhs.member < rhs.member;
              });
    ++key_versions_[key];
    return int64_t{0};
  }

  entries.push_back(SortedSetEntry{
      .member = member, .score = score, .score_text = score_text});
  std::sort(entries.begin(), entries.end(),
            [](const SortedSetEntry &lhs, const SortedSetEntry &rhs) {
              if (lhs.score != rhs.score) {
                return lhs.score < rhs.score;
              }
              return lhs.member < rhs.member;
            });
  ++key_versions_[key];
  return int64_t{1};
}

DatabaseResult<std::optional<int64_t>>
Database::ZRank(const std::string &key, const std::string &member) {
  std::lock_guard<std::mutex> lock(mutex_);
  Entry *entry = FindLiveEntryLocked(key);
  if (entry == nullptr) {
    return std::optional<int64_t>{};
  }

  if (!std::holds_alternative<SortedSetValue>(entry->value)) {
    return tl::make_unexpected(DatabaseError{
        .code = DatabaseErrorCode::kWrongType,
        .actual_type = EntryValueType(*entry),
    });
  }

  const std::vector<SortedSetEntry> &entries =
      std::get<SortedSetValue>(entry->value).entries;
  for (size_t index = 0; index < entries.size(); ++index) {
    if (entries[index].member == member) {
      return std::optional<int64_t>{static_cast<int64_t>(index)};
    }
  }

  return std::optional<int64_t>{};
}

DatabaseResult<std::vector<std::string>>
Database::ZRange(const std::string &key, int64_t start, int64_t stop) {
  std::lock_guard<std::mutex> lock(mutex_);
  Entry *entry = FindLiveEntryLocked(key);
  if (entry == nullptr) {
    return std::vector<std::string>{};
  }

  if (!std::holds_alternative<SortedSetValue>(entry->value)) {
    return tl::make_unexpected(DatabaseError{
        .code = DatabaseErrorCode::kWrongType,
        .actual_type = EntryValueType(*entry),
    });
  }

  const std::vector<SortedSetEntry> &entries =
      std::get<SortedSetValue>(entry->value).entries;
  const int64_t entry_count = static_cast<int64_t>(entries.size());

  if (start < 0) {
    start += entry_count;
  }
  if (stop < 0) {
    stop += entry_count;
  }
  if (start < 0) {
    start = 0;
  }
  if (stop < 0) {
    return std::vector<std::string>{};
  }
  if (start >= entry_count || start > stop) {
    return std::vector<std::string>{};
  }
  if (stop >= entry_count) {
    stop = entry_count - 1;
  }

  std::vector<std::string> members;
  members.reserve(static_cast<size_t>(stop - start + 1));
  for (int64_t index = start; index <= stop; ++index) {
    members.push_back(entries[static_cast<size_t>(index)].member);
  }

  return members;
}

DatabaseResult<int64_t> Database::ZCard(const std::string &key) {
  std::lock_guard<std::mutex> lock(mutex_);
  Entry *entry = FindLiveEntryLocked(key);
  if (entry == nullptr) {
    return int64_t{0};
  }

  if (!std::holds_alternative<SortedSetValue>(entry->value)) {
    return tl::make_unexpected(DatabaseError{
        .code = DatabaseErrorCode::kWrongType,
        .actual_type = EntryValueType(*entry),
    });
  }

  return static_cast<int64_t>(
      std::get<SortedSetValue>(entry->value).entries.size());
}

DatabaseResult<std::optional<std::string>>
Database::ZScore(const std::string &key, const std::string &member) {
  std::lock_guard<std::mutex> lock(mutex_);
  Entry *entry = FindLiveEntryLocked(key);
  if (entry == nullptr) {
    return std::optional<std::string>{};
  }

  if (!std::holds_alternative<SortedSetValue>(entry->value)) {
    return tl::make_unexpected(DatabaseError{
        .code = DatabaseErrorCode::kWrongType,
        .actual_type = EntryValueType(*entry),
    });
  }

  const std::vector<SortedSetEntry> &entries =
      std::get<SortedSetValue>(entry->value).entries;
  for (const SortedSetEntry &existing : entries) {
    if (existing.member == member) {
      return std::optional<std::string>{existing.score_text};
    }
  }

  return std::optional<std::string>{};
}

DatabaseResult<int64_t> Database::ZRem(const std::string &key,
                                       const std::string &member) {
  std::lock_guard<std::mutex> lock(mutex_);
  Entry *entry = FindLiveEntryLocked(key);
  if (entry == nullptr) {
    return int64_t{0};
  }

  if (!std::holds_alternative<SortedSetValue>(entry->value)) {
    return tl::make_unexpected(DatabaseError{
        .code = DatabaseErrorCode::kWrongType,
        .actual_type = EntryValueType(*entry),
    });
  }

  std::vector<SortedSetEntry> &entries =
      std::get<SortedSetValue>(entry->value).entries;
  const auto it = std::find_if(entries.begin(), entries.end(),
                               [&](const SortedSetEntry &existing) {
                                 return existing.member == member;
                               });
  if (it == entries.end()) {
    return int64_t{0};
  }

  entries.erase(it);
  ++key_versions_[key];
  return int64_t{1};
}

uint64_t Database::KeyVersion(const std::string &key) {
  std::lock_guard<std::mutex> lock(mutex_);
  (void)FindLiveEntryLocked(key);
  const auto found = key_versions_.find(key);
  return found == key_versions_.end() ? 0 : found->second;
}

DatabaseResult<std::vector<std::pair<std::string, std::string>>>
Database::ZEntries(const std::string &key) {
  std::lock_guard<std::mutex> lock(mutex_);
  Entry *entry = FindLiveEntryLocked(key);
  if (entry == nullptr) {
    return std::vector<std::pair<std::string, std::string>>{};
  }

  if (!std::holds_alternative<SortedSetValue>(entry->value)) {
    return tl::make_unexpected(DatabaseError{
        .code = DatabaseErrorCode::kWrongType,
        .actual_type = EntryValueType(*entry),
    });
  }

  const std::vector<SortedSetEntry> &stored_entries =
      std::get<SortedSetValue>(entry->value).entries;
  std::vector<std::pair<std::string, std::string>> entries;
  entries.reserve(stored_entries.size());
  for (const SortedSetEntry &stored_entry : stored_entries) {
    entries.emplace_back(stored_entry.member, stored_entry.score_text);
  }

  return entries;
}

Database::Entry *Database::FindLiveEntryLocked(const std::string &key) {
  const auto found = store_.find(key);
  if (found == store_.end()) {
    return nullptr;
  }

  const auto now = std::chrono::steady_clock::now();
  if (IsExpired(found->second, now)) {
    store_.erase(found);
    ++key_versions_[key];
    return nullptr;
  }

  return &found->second;
}

std::optional<Database::StreamId>
Database::ParseStreamId(std::string_view value) {
  const size_t separator = value.find('-');
  if (separator == std::string_view::npos || separator == 0 ||
      separator == value.size() - 1) {
    return std::nullopt;
  }

  const std::optional<int64_t> milliseconds =
      ParseNonNegativeInteger(value.substr(0, separator));
  const std::optional<int64_t> sequence =
      ParseNonNegativeInteger(value.substr(separator + 1));
  if (!milliseconds.has_value() || !sequence.has_value()) {
    return std::nullopt;
  }
  return StreamId{.milliseconds = *milliseconds, .sequence = *sequence};
}

std::optional<Database::ParsedXAddStreamId>
Database::ParseXAddStreamId(std::string_view value) {
  if (value == "*") {
    return ParsedXAddStreamId{
        .id = {},
        .generation = XAddIdGenerationPolicy::kMillisecondsAndSequence,
    };
  }

  const std::optional<StreamId> exact_id = ParseStreamId(value);
  if (exact_id.has_value()) {
    return ParsedXAddStreamId{.id = *exact_id};
  }

  const size_t separator = value.find('-');
  if (separator == std::string_view::npos || separator == 0 ||
      separator != value.size() - 2 || value.back() != '*') {
    return std::nullopt;
  }

  const std::optional<int64_t> milliseconds =
      ParseNonNegativeInteger(value.substr(0, separator));
  if (!milliseconds.has_value()) {
    return std::nullopt;
  }

  return ParsedXAddStreamId{
      .id = StreamId{.milliseconds = *milliseconds, .sequence = 0},
      .generation = XAddIdGenerationPolicy::kSequence,
  };
}

std::optional<Database::StreamId>
Database::ParseStreamRangeId(std::string_view value, StreamRangeBound bound) {
  if (value == "-") {
    return StreamId{};
  }
  if (value == "+") {
    return StreamId{
        .milliseconds = std::numeric_limits<int64_t>::max(),
        .sequence = std::numeric_limits<int64_t>::max(),
    };
  }

  const std::optional<StreamId> exact_id = ParseStreamId(value);
  if (exact_id.has_value()) {
    return exact_id;
  }

  const std::optional<int64_t> milliseconds =
      ParseNonNegativeInteger(value);
  if (!milliseconds.has_value()) {
    return std::nullopt;
  }

  return StreamId{
      .milliseconds = *milliseconds,
      .sequence = bound == StreamRangeBound::kStart
                      ? 0
                      : std::numeric_limits<int64_t>::max(),
  };
}

std::string Database::FormatStreamId(const StreamId &id) {
  return std::to_string(id.milliseconds) + "-" + std::to_string(id.sequence);
}

int Database::CompareStreamIds(const StreamId &lhs, const StreamId &rhs) {
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

ValueType Database::EntryValueType(const Entry &entry) {
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

bool Database::IsExpired(const Entry &entry,
                         std::chrono::steady_clock::time_point now) {
  return entry.expires_at.has_value() && now >= *entry.expires_at;
}

} // namespace redis
