#include "redis-cpp/pubsub_manager.hpp"

namespace redis {

void PubSubManager::Subscribe(int fd, const std::string& channel) {
  std::lock_guard<std::mutex> lock(mutex_);
  channel_subscribers_[channel].insert(fd);
}

void PubSubManager::Unsubscribe(int fd, const std::string& channel) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto found = channel_subscribers_.find(channel);
  if (found == channel_subscribers_.end()) {
    return;
  }

  found->second.erase(fd);
  if (found->second.empty()) {
    channel_subscribers_.erase(found);
  }
}

int64_t PubSubManager::SubscriberCount(const std::string& channel) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto found = channel_subscribers_.find(channel);
  if (found == channel_subscribers_.end()) {
    return 0;
  }
  return static_cast<int64_t>(found->second.size());
}

}  // namespace redis
