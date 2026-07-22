#pragma once

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "redis-cpp/result.hpp"
#include "redis-cpp/unique_fd.hpp"

namespace redis {

class ReplicaManager {
 public:
  Status AddReplica(int connection_fd);
  void RemoveReplica(int connection_fd);
  void PropagateToAll(const std::string& data);
  size_t ReplicaCount();
  bool UpdateReplicaAck(int fd, int64_t offset);
  int64_t WaitForReplicas(size_t required,
                          std::chrono::milliseconds timeout);

 private:
  struct ReplicaState {
    int connection_fd = -1;
    UniqueFd send_fd;
    int64_t acknowledged_offset = 0;
  };

  int64_t CountAcknowledgedReplicasLocked(int64_t target_offset) const;
  std::vector<std::pair<int, int>> SnapshotReplicaFdsLocked() const;
  std::vector<int> SendToReplicas(
      const std::vector<std::pair<int, int>>& replica_fds,
      const std::string& data) const;
  void RemoveReplicasLocked(const std::vector<int>& connection_fds);

  std::mutex mutex_;
  std::mutex send_mutex_;
  std::condition_variable replica_ack_cv_;
  std::vector<ReplicaState> replicas_;
  int64_t write_offset_ = 0;
};

}  // namespace redis
