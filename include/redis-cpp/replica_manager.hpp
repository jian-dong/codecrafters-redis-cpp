#pragma once

#include <mutex>
#include <string>
#include <vector>

namespace redis {

class ReplicaManager {
 public:
  void AddReplica(int fd);
  void PropagateToAll(const std::string& data);

 private:
  std::mutex mutex_;
  std::vector<int> replica_fds_;
};

}  // namespace redis
