#include "redis-cpp/replica_manager.hpp"

#include <sys/socket.h>

namespace redis {

void ReplicaManager::AddReplica(int fd) {
  std::lock_guard<std::mutex> lock(mutex_);
  replica_fds_.push_back(fd);
}

void ReplicaManager::PropagateToAll(const std::string& data) {
  std::lock_guard<std::mutex> lock(mutex_);
  for (const int fd : replica_fds_) {
    size_t total_sent = 0;
    while (total_sent < data.size()) {
      const ssize_t sent =
          send(fd, data.data() + total_sent, data.size() - total_sent, 0);
      if (sent <= 0) {
        break;
      }
      total_sent += static_cast<size_t>(sent);
    }
  }
}

}  // namespace redis
