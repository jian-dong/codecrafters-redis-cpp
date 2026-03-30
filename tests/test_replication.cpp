#include "test_support.hpp"

namespace {

using redis::CommandExecutor;
using redis::Database;
using redis::ReplicaManager;
using redis::RespArray;
using redis::RespInteger;
using redis::RespSimpleString;
using redis::RespWriter;

TEST(ReplicationTest, ReplicaReplconfGetackReturnsAck) {
  Database database;
  CommandExecutor executor(database, true);

  redis::CommandResult result = executor.Execute({"REPLCONF", "GETACK", "*"});
  ASSERT_TRUE((result.has_value())) << "replica REPLCONF GETACK should succeed";
  ASSERT_TRUE((std::holds_alternative<RespArray>(*result))) << "replica REPLCONF GETACK should return a RESP array";

  const auto& response = std::get<RespArray>(*result);
  ASSERT_TRUE((response.values == std::vector<std::string>({"REPLCONF", "ACK", "0"}))) << "replica REPLCONF GETACK should return REPLCONF ACK 0";
  ASSERT_TRUE((RespWriter::Write(*result) ==
             "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n")) << "replica REPLCONF GETACK should encode as the expected RESP array";
}

TEST(ReplicationTest, MasterReplconfStillReturnsOk) {
  Database database;
  CommandExecutor executor(database, false);

  redis::CommandResult result = executor.Execute({"REPLCONF", "GETACK", "*"});
  ASSERT_TRUE((result.has_value())) << "master REPLCONF GETACK should succeed";
  ASSERT_TRUE((std::holds_alternative<RespSimpleString>(*result))) << "master REPLCONF GETACK should still return +OK";
  ASSERT_TRUE((std::get<RespSimpleString>(*result).value == "OK")) << "master REPLCONF GETACK should return OK";
}

TEST(ReplicationTest, WaitReturnsZeroImmediatelyWithoutReplicas) {
  Database database;
  CommandExecutor executor(database, false);

  redis::CommandResult result = executor.Execute({"WAIT", "0", "60000"});
  ASSERT_TRUE((result.has_value())) << "WAIT 0 60000 should succeed";
  ASSERT_TRUE((std::holds_alternative<RespInteger>(*result))) << "WAIT 0 60000 should return a RESP integer";
  ASSERT_TRUE((std::get<RespInteger>(*result).value == 0)) << "WAIT 0 60000 should return 0";
  ASSERT_TRUE((RespWriter::Write(*result) == ":0\r\n")) << "WAIT 0 60000 should encode as :0";
}

TEST(ReplicationTest, WaitReturnsConnectedReplicaCount) {
  Database database;
  ReplicaManager replica_manager;
  CommandExecutor executor(database, false, &replica_manager);

  int replica_sockets[2];
  const int socket_pair_status =
      socketpair(AF_UNIX, SOCK_STREAM, 0, replica_sockets);
  ASSERT_TRUE((socket_pair_status == 0)) << "socketpair should succeed";

  replica_manager.AddReplica(replica_sockets[0]);
  replica_manager.AddReplica(replica_sockets[1]);

  redis::CommandResult result = executor.Execute({"WAIT", "9", "500"});
  ASSERT_TRUE((result.has_value())) << "WAIT 9 500 should succeed";
  ASSERT_TRUE((std::holds_alternative<RespInteger>(*result))) << "WAIT 9 500 should return a RESP integer";
  ASSERT_TRUE((std::get<RespInteger>(*result).value == 2)) << "WAIT should return the connected replica count";
  ASSERT_TRUE((RespWriter::Write(*result) == ":2\r\n")) << "WAIT should encode the connected replica count as a RESP integer";

  close(replica_sockets[0]);
  close(replica_sockets[1]);
}

TEST(ReplicationTest, WaitBlocksUntilReplicaAcknowledgesPreviousWrite) {
  Database database;
  ReplicaManager replica_manager;
  CommandExecutor executor(database, false, &replica_manager);

  int replica_sockets[2];
  const int socket_pair_status =
      socketpair(AF_UNIX, SOCK_STREAM, 0, replica_sockets);
  ASSERT_TRUE((socket_pair_status == 0)) << "socketpair should succeed";

  replica_manager.AddReplica(replica_sockets[0]);

  const std::string write_command =
      "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n123\r\n";
  replica_manager.PropagateToAll(write_command);

  char buffer[256];
  const ssize_t write_bytes = recv(replica_sockets[1], buffer, sizeof(buffer), 0);
  ASSERT_TRUE((write_bytes == static_cast<ssize_t>(write_command.size()))) << "replica should receive the propagated write command";

  auto wait_result = std::async(std::launch::async, [&]() {
    return executor.Execute({"WAIT", "1", "200"});
  });

  const std::string expected_getack =
      "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
  const ssize_t getack_bytes = recv(replica_sockets[1], buffer, sizeof(buffer), 0);
  ASSERT_TRUE((getack_bytes == static_cast<ssize_t>(expected_getack.size()))) << "WAIT should request acknowledgements from replicas";
  ASSERT_TRUE((std::string(buffer, buffer + getack_bytes) == expected_getack)) << "WAIT should send REPLCONF GETACK *";

  const bool updated = replica_manager.UpdateReplicaAck(
      replica_sockets[0], static_cast<int64_t>(write_command.size()));
  ASSERT_TRUE((updated)) << "replica acknowledgement should update the replica manager";

  redis::CommandResult result = wait_result.get();
  ASSERT_TRUE((result.has_value())) << "WAIT should succeed after the replica ACKs";
  ASSERT_TRUE((std::holds_alternative<RespInteger>(*result))) << "WAIT should return a RESP integer after waiting";
  ASSERT_TRUE((std::get<RespInteger>(*result).value == 1)) << "WAIT should report the replica that acknowledged the write";

  close(replica_sockets[0]);
  close(replica_sockets[1]);
}

}  // namespace
