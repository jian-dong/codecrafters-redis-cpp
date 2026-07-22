#include "test_support.hpp"

#include <future>
#include <string>

#include "redis-cpp/replica_manager.hpp"

namespace {

TEST(ReplicaManagerTest, OwnsAnIndependentSendHandle) {
  int sockets[2];
  ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);

  redis::ReplicaManager manager;
  ASSERT_TRUE(manager.AddReplica(sockets[0]).has_value());
  close(sockets[0]);

  const std::string payload = "propagated command";
  manager.PropagateToAll(payload);

  std::string received(payload.size(), '\0');
  ASSERT_EQ(recv(sockets[1], received.data(), received.size(), 0),
            static_cast<ssize_t>(payload.size()));
  EXPECT_EQ(received, payload);
  close(sockets[1]);
}

TEST(ReplicaManagerTest, RemovingAReplicaReleasesItsOwnedHandle) {
  int sockets[2];
  ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);

  redis::ReplicaManager manager;
  ASSERT_TRUE(manager.AddReplica(sockets[0]).has_value());
  close(sockets[0]);
  manager.RemoveReplica(sockets[0]);

  EXPECT_EQ(manager.ReplicaCount(), 0U);
  char byte = '\0';
  EXPECT_EQ(recv(sockets[1], &byte, 1, 0), 0);
  close(sockets[1]);
}

TEST(ReplicaManagerTest, RejectsAnInvalidConnectionHandle) {
  redis::ReplicaManager manager;

  const redis::Status result = manager.AddReplica(-1);

  ASSERT_FALSE(result.has_value());
  const auto* error =
      std::get_if<redis::NetworkError>(&result.error().Kind());
  ASSERT_NE(error, nullptr);
  EXPECT_EQ(error->code, redis::NetworkErrorCode::kSocketDuplicateFailed);
}

}  // namespace
