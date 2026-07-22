#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "redis-cpp/socket.hpp"

namespace redis {

class PubSubManager {
 public:
  void Subscribe(const SharedConnectionWriter& writer,
                 const std::string& channel);
  void Unsubscribe(ConnectionId connection_id, const std::string& channel);
  int64_t SubscriberCount(const std::string& channel);
  int64_t Publish(const std::string& channel, const std::string& message);

 private:
  using Subscribers =
      std::unordered_map<ConnectionId, std::weak_ptr<ConnectionWriter>>;

  std::mutex mutex_;
  std::unordered_map<std::string, Subscribers> channel_subscribers_;
};

}  // namespace redis
