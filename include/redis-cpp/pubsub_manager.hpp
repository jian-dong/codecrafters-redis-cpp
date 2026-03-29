#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace redis {

class PubSubManager {
 public:
  void Subscribe(int fd, const std::string& channel);
  void Unsubscribe(int fd, const std::string& channel);
  int64_t SubscriberCount(const std::string& channel);
  int64_t Publish(const std::string& channel, const std::string& message);

 private:
  std::mutex mutex_;
  std::unordered_map<std::string, std::unordered_set<int>> channel_subscribers_;
};

}  // namespace redis
