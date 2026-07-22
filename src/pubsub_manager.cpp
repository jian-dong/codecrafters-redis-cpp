#include "redis-cpp/pubsub_manager.hpp"

#include <memory>
#include <utility>
#include <vector>

#include "redis-cpp/resp.hpp"

namespace redis {
namespace {

std::string EncodeMessageFrame(const std::string& channel,
                               const std::string& message) {
  return RespWriter::Write(
      RespArray::BulkStrings({"message", channel, message}));
}

}  // namespace

void PubSubManager::Subscribe(const SharedConnectionWriter& writer,
                              const std::string& channel) {
  if (writer == nullptr || !writer->IsOpen()) {
    return;
  }
  std::lock_guard<std::mutex> lock(mutex_);
  channel_subscribers_[channel].insert_or_assign(writer->Id(), writer);
}

void PubSubManager::Unsubscribe(ConnectionId connection_id,
                                const std::string& channel) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto found = channel_subscribers_.find(channel);
  if (found == channel_subscribers_.end()) {
    return;
  }

  found->second.erase(connection_id);
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
  int64_t count = 0;
  for (auto subscriber = found->second.begin();
       subscriber != found->second.end();) {
    const std::shared_ptr<ConnectionWriter> writer = subscriber->second.lock();
    if (writer == nullptr || !writer->IsOpen()) {
      subscriber = found->second.erase(subscriber);
    } else {
      ++count;
      ++subscriber;
    }
  }
  if (found->second.empty()) {
    channel_subscribers_.erase(found);
  }
  return count;
}

int64_t PubSubManager::Publish(const std::string& channel,
                               const std::string& message) {
  std::vector<std::pair<ConnectionId, SharedConnectionWriter>> subscribers;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto found = channel_subscribers_.find(channel);
    if (found == channel_subscribers_.end()) {
      return 0;
    }
    subscribers.reserve(found->second.size());
    for (auto subscriber = found->second.begin();
         subscriber != found->second.end();) {
      SharedConnectionWriter writer = subscriber->second.lock();
      if (writer == nullptr || !writer->IsOpen()) {
        subscriber = found->second.erase(subscriber);
        continue;
      }
      subscribers.emplace_back(subscriber->first, std::move(writer));
      ++subscriber;
    }
    if (found->second.empty()) {
      channel_subscribers_.erase(found);
    }
  }

  const std::string frame = EncodeMessageFrame(channel, message);
  std::vector<ConnectionId> disconnected;
  int64_t delivered = 0;
  for (const auto& [connection_id, writer] : subscribers) {
    if (writer->SendAll(frame)) {
      ++delivered;
    } else {
      disconnected.push_back(connection_id);
    }
  }

  if (!disconnected.empty()) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto found = channel_subscribers_.find(channel);
    if (found != channel_subscribers_.end()) {
      for (ConnectionId connection_id : disconnected) {
        found->second.erase(connection_id);
      }
      if (found->second.empty()) {
        channel_subscribers_.erase(found);
      }
    }
  }

  return delivered;
}

}  // namespace redis
