#include "redis-cpp/replica_manager.hpp"

#include <sys/socket.h>

#include <string_view>

namespace redis {
namespace {

constexpr std::string_view kReplconfGetackCommand =
    "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";

}  // namespace

void ReplicaManager::AddReplica(int fd) {
  std::lock_guard<std::mutex> lock(mutex_);
  replicas_.push_back(
      ReplicaState{.fd = fd, .acknowledged_offset = write_offset_});
}

void ReplicaManager::PropagateToAll(const std::string& data) {
  std::vector<int> replica_fds;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    write_offset_ += static_cast<int64_t>(data.size());
    replica_fds.reserve(replicas_.size());
    for (const ReplicaState& replica : replicas_) {
      replica_fds.push_back(replica.fd);
    }
  }

  SendToReplicas(replica_fds, data);
}

size_t ReplicaManager::ReplicaCount() {
  std::lock_guard<std::mutex> lock(mutex_);
  return replicas_.size();
}

bool ReplicaManager::UpdateReplicaAck(int fd, int64_t offset) {
  std::lock_guard<std::mutex> lock(mutex_);
  for (ReplicaState& replica : replicas_) {
    if (replica.fd != fd) {
      continue;
    }

    if (offset > replica.acknowledged_offset) {
      replica.acknowledged_offset = offset;
      replica_ack_cv_.notify_all();
    }
    return true;
  }

  return false;
}

int64_t ReplicaManager::WaitForReplicas(size_t required,
                                        std::chrono::milliseconds timeout) {
  int64_t target_offset = 0;
  int64_t acknowledged_count = 0;
  std::vector<int> replica_fds;
  {
    std::unique_lock<std::mutex> lock(mutex_);
    target_offset = write_offset_;
    acknowledged_count = CountAcknowledgedReplicasLocked(target_offset);
    if (acknowledged_count >= static_cast<int64_t>(required) ||
        target_offset == 0) {
      return acknowledged_count;
    }

    replica_fds.reserve(replicas_.size());
    for (const ReplicaState& replica : replicas_) {
      replica_fds.push_back(replica.fd);
    }
  }

  SendToReplicas(replica_fds, std::string(kReplconfGetackCommand));

  std::unique_lock<std::mutex> lock(mutex_);
  replica_ack_cv_.wait_for(lock, timeout, [&] {
    return CountAcknowledgedReplicasLocked(target_offset) >=
           static_cast<int64_t>(required);
  });
  return CountAcknowledgedReplicasLocked(target_offset);
}

int64_t ReplicaManager::CountAcknowledgedReplicasLocked(
    int64_t target_offset) const {
  int64_t count = 0;
  for (const ReplicaState& replica : replicas_) {
    if (replica.acknowledged_offset >= target_offset) {
      ++count;
    }
  }
  return count;
}

void ReplicaManager::SendToReplicas(const std::vector<int>& replica_fds,
                                    const std::string& data) const {
  for (const int fd : replica_fds) {
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
