#include "redis-cpp/pubsub_manager.hpp"

#include <sys/socket.h>

#include <vector>

namespace redis {
namespace {

std::string EncodeMessageFrame(const std::string& channel,
                               const std::string& message) {
  std::string response = "*3\r\n";
  response += "$7\r\nmessage\r\n";
  response += "$" + std::to_string(channel.size()) + "\r\n" + channel + "\r\n";
  response += "$" + std::to_string(message.size()) + "\r\n" + message + "\r\n";
  return response;
}

void SendAll(int fd, const std::string& data) {
  size_t total_sent = 0;
  while (total_sent < data.size()) {
    const ssize_t sent =
        send(fd, data.data() + total_sent, data.size() - total_sent, 0);
    if (sent <= 0) {
      return;
    }
    total_sent += static_cast<size_t>(sent);
  }
}

}  // namespace

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

int64_t PubSubManager::Publish(const std::string& channel,
                               const std::string& message) {
  std::vector<int> subscribers;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto found = channel_subscribers_.find(channel);
    if (found == channel_subscribers_.end()) {
      return 0;
    }
    subscribers.assign(found->second.begin(), found->second.end());
  }

  const std::string frame = EncodeMessageFrame(channel, message);
  for (const int fd : subscribers) {
    SendAll(fd, frame);
  }

  return static_cast<int64_t>(subscribers.size());
}

}  // namespace redis
