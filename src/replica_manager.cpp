#include "redis-cpp/replica_manager.hpp"

#include <algorithm>
#include <cerrno>
#include <utility>

#include <sys/socket.h>
#include <unistd.h>

#include "redis-cpp/resp.hpp"

namespace redis {
namespace {

void DisableSigpipe(int fd) {
#ifdef SO_NOSIGPIPE
  const int enabled = 1;
  (void)setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &enabled, sizeof(enabled));
#else
  (void)fd;
#endif
}

ssize_t SendWithoutSigpipe(int fd, const void* data, size_t size) {
#ifdef MSG_NOSIGNAL
  return send(fd, data, size, MSG_NOSIGNAL);
#else
  return send(fd, data, size, 0);
#endif
}

}  // namespace

Status ReplicaManager::AddReplica(int connection_fd) {
  const int duplicated_fd = dup(connection_fd);
  if (duplicated_fd < 0) {
    return tl::make_unexpected(
        MakeNetworkError(NetworkErrorCode::kSocketDuplicateFailed));
  }
  DisableSigpipe(duplicated_fd);
  UniqueFd owned_send_fd(duplicated_fd);

  std::lock_guard<std::mutex> send_lock(send_mutex_);
  std::lock_guard<std::mutex> lock(mutex_);
  const auto existing = std::find_if(
      replicas_.begin(), replicas_.end(), [&](const ReplicaState& replica) {
        return replica.connection_fd == connection_fd;
      });
  if (existing != replicas_.end()) {
    return {};
  }
  replicas_.push_back(ReplicaState{.connection_fd = connection_fd,
                                   .send_fd = std::move(owned_send_fd),
                                   .acknowledged_offset = write_offset_});
  return {};
}

void ReplicaManager::RemoveReplica(int connection_fd) {
  std::lock_guard<std::mutex> send_lock(send_mutex_);
  std::lock_guard<std::mutex> state_lock(mutex_);
  RemoveReplicasLocked({connection_fd});
}

void ReplicaManager::PropagateToAll(const std::string& data) {
  std::lock_guard<std::mutex> send_lock(send_mutex_);
  std::vector<std::pair<int, int>> replica_fds;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    write_offset_ += static_cast<int64_t>(data.size());
    replica_fds = SnapshotReplicaFdsLocked();
  }

  const std::vector<int> disconnected = SendToReplicas(replica_fds, data);
  if (!disconnected.empty()) {
    std::lock_guard<std::mutex> lock(mutex_);
    RemoveReplicasLocked(disconnected);
  }
}

size_t ReplicaManager::ReplicaCount() {
  std::lock_guard<std::mutex> lock(mutex_);
  return replicas_.size();
}

bool ReplicaManager::UpdateReplicaAck(int fd, int64_t offset) {
  std::lock_guard<std::mutex> lock(mutex_);
  for (ReplicaState& replica : replicas_) {
    if (replica.connection_fd != fd) {
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
  {
    std::unique_lock<std::mutex> lock(mutex_);
    target_offset = write_offset_;
    acknowledged_count = CountAcknowledgedReplicasLocked(target_offset);
    if (acknowledged_count >= static_cast<int64_t>(required) ||
        target_offset == 0) {
      return acknowledged_count;
    }
  }

  {
    std::lock_guard<std::mutex> send_lock(send_mutex_);
    std::vector<std::pair<int, int>> replica_fds;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      replica_fds = SnapshotReplicaFdsLocked();
    }
    const std::vector<int> disconnected = SendToReplicas(
        replica_fds,
        RespWriter::WriteCommand({"REPLCONF", "GETACK", "*"}));
    if (!disconnected.empty()) {
      std::lock_guard<std::mutex> lock(mutex_);
      RemoveReplicasLocked(disconnected);
    }
  }

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

std::vector<std::pair<int, int>> ReplicaManager::SnapshotReplicaFdsLocked()
    const {
  std::vector<std::pair<int, int>> replica_fds;
  replica_fds.reserve(replicas_.size());
  for (const ReplicaState& replica : replicas_) {
    replica_fds.emplace_back(replica.connection_fd, replica.send_fd.Get());
  }
  return replica_fds;
}

std::vector<int> ReplicaManager::SendToReplicas(
    const std::vector<std::pair<int, int>>& replica_fds,
    const std::string& data) const {
  std::vector<int> disconnected;
  for (const auto& [connection_fd, send_fd] : replica_fds) {
    size_t total_sent = 0;
    while (total_sent < data.size()) {
      const ssize_t sent = SendWithoutSigpipe(
          send_fd, data.data() + total_sent, data.size() - total_sent);
      if (sent < 0 && errno == EINTR) {
        continue;
      }
      if (sent <= 0) {
        disconnected.push_back(connection_fd);
        break;
      }
      total_sent += static_cast<size_t>(sent);
    }
  }
  return disconnected;
}

void ReplicaManager::RemoveReplicasLocked(
    const std::vector<int>& connection_fds) {
  replicas_.erase(
      std::remove_if(replicas_.begin(), replicas_.end(),
                     [&](const ReplicaState& replica) {
                       return std::find(connection_fds.begin(),
                                        connection_fds.end(),
                                        replica.connection_fd) !=
                              connection_fds.end();
                     }),
      replicas_.end());
}

}  // namespace redis
