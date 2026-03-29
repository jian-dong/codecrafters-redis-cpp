#pragma once

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <vector>

namespace redis {

class ReplicaManager {
 public:
  void AddReplica(int fd);
  void PropagateToAll(const std::string& data);
  size_t ReplicaCount();
  bool UpdateReplicaAck(int fd, int64_t offset);
  int64_t WaitForReplicas(size_t required,
                          std::chrono::milliseconds timeout);

 private:
  struct ReplicaState {
    int fd = -1;
    int64_t acknowledged_offset = 0;
  };

  int64_t CountAcknowledgedReplicasLocked(int64_t target_offset) const;
  void SendToReplicas(const std::vector<int>& replica_fds,
                      const std::string& data) const;

  std::mutex mutex_;
  std::condition_variable replica_ack_cv_;
  std::vector<ReplicaState> replicas_;
  int64_t write_offset_ = 0;
};

}  // namespace redis
