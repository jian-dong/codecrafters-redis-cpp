#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <sys/socket.h>
#include <unistd.h>
#include <string>
#include <vector>

#include "redis-cpp/command_processor.hpp"
#include "redis-cpp/replica_manager.hpp"
#include "redis-cpp/rdb_loader.hpp"

namespace {

using redis::CommandProcessor;
using redis::Database;
using redis::ReplicaManager;
using redis::RespArray;
using redis::RespInteger;
using redis::RespSimpleString;
using redis::RespWriter;
using redis::ServerConfig;

void Expect(bool condition, const std::string& message) {
  if (!condition) {
    std::cerr << message << "\n";
    std::exit(1);
  }
}

void TestReplicaReplconfGetackReturnsAck() {
  Database database;
  CommandProcessor processor(database, true);

  redis::CommandResult result = processor.Execute({"REPLCONF", "GETACK", "*"});
  Expect(result.has_value(), "replica REPLCONF GETACK should succeed");
  Expect(std::holds_alternative<RespArray>(*result),
         "replica REPLCONF GETACK should return a RESP array");

  const auto& response = std::get<RespArray>(*result);
  Expect(response.values ==
             std::vector<std::string>({"REPLCONF", "ACK", "0"}),
         "replica REPLCONF GETACK should return REPLCONF ACK 0");
  Expect(RespWriter::Write(*result) ==
             "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n",
         "replica REPLCONF GETACK should encode as the expected RESP array");
}

void TestMasterReplconfStillReturnsOk() {
  Database database;
  CommandProcessor processor(database, false);

  redis::CommandResult result = processor.Execute({"REPLCONF", "GETACK", "*"});
  Expect(result.has_value(), "master REPLCONF GETACK should succeed");
  Expect(std::holds_alternative<RespSimpleString>(*result),
         "master REPLCONF GETACK should still return +OK");
  Expect(std::get<RespSimpleString>(*result).value == "OK",
         "master REPLCONF GETACK should return OK");
}

void TestWaitReturnsZeroImmediatelyWithoutReplicas() {
  Database database;
  CommandProcessor processor(database, false);

  redis::CommandResult result = processor.Execute({"WAIT", "0", "60000"});
  Expect(result.has_value(), "WAIT 0 60000 should succeed");
  Expect(std::holds_alternative<RespInteger>(*result),
         "WAIT 0 60000 should return a RESP integer");
  Expect(std::get<RespInteger>(*result).value == 0,
         "WAIT 0 60000 should return 0");
  Expect(RespWriter::Write(*result) == ":0\r\n",
         "WAIT 0 60000 should encode as :0");
}

void TestWaitReturnsConnectedReplicaCount() {
  Database database;
  ReplicaManager replica_manager;
  CommandProcessor processor(database, false, &replica_manager);

  int replica_sockets[2];
  const int socket_pair_status =
      socketpair(AF_UNIX, SOCK_STREAM, 0, replica_sockets);
  Expect(socket_pair_status == 0, "socketpair should succeed");

  replica_manager.AddReplica(replica_sockets[0]);
  replica_manager.AddReplica(replica_sockets[1]);

  redis::CommandResult result = processor.Execute({"WAIT", "9", "500"});
  Expect(result.has_value(), "WAIT 9 500 should succeed");
  Expect(std::holds_alternative<RespInteger>(*result),
         "WAIT 9 500 should return a RESP integer");
  Expect(std::get<RespInteger>(*result).value == 2,
         "WAIT should return the connected replica count");
  Expect(RespWriter::Write(*result) == ":2\r\n",
         "WAIT should encode the connected replica count as a RESP integer");

  close(replica_sockets[0]);
  close(replica_sockets[1]);
}

void TestWaitBlocksUntilReplicaAcknowledgesPreviousWrite() {
  Database database;
  ReplicaManager replica_manager;
  CommandProcessor processor(database, false, &replica_manager);

  int replica_sockets[2];
  const int socket_pair_status =
      socketpair(AF_UNIX, SOCK_STREAM, 0, replica_sockets);
  Expect(socket_pair_status == 0, "socketpair should succeed");

  replica_manager.AddReplica(replica_sockets[0]);

  const std::string write_command =
      "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n123\r\n";
  replica_manager.PropagateToAll(write_command);

  char buffer[256];
  const ssize_t write_bytes =
      recv(replica_sockets[1], buffer, sizeof(buffer), 0);
  Expect(write_bytes == static_cast<ssize_t>(write_command.size()),
         "replica should receive the propagated write command");

  auto wait_result = std::async(std::launch::async, [&]() {
    return processor.Execute({"WAIT", "1", "200"});
  });

  const std::string expected_getack =
      "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
  const ssize_t getack_bytes = recv(replica_sockets[1], buffer, sizeof(buffer), 0);
  Expect(getack_bytes == static_cast<ssize_t>(expected_getack.size()),
         "WAIT should request acknowledgements from replicas");
  Expect(std::string(buffer, buffer + getack_bytes) == expected_getack,
         "WAIT should send REPLCONF GETACK *");

  const bool updated = replica_manager.UpdateReplicaAck(
      replica_sockets[0], static_cast<int64_t>(write_command.size()));
  Expect(updated, "replica acknowledgement should update the replica manager");

  redis::CommandResult result = wait_result.get();
  Expect(result.has_value(), "WAIT should succeed after the replica ACKs");
  Expect(std::holds_alternative<RespInteger>(*result),
         "WAIT should return a RESP integer after waiting");
  Expect(std::get<RespInteger>(*result).value == 1,
         "WAIT should report the replica that acknowledged the write");

  close(replica_sockets[0]);
  close(replica_sockets[1]);
}

void TestConfigGetDirReturnsConfiguredDirectory() {
  Database database;
  ServerConfig config;
  config.dir = "/tmp/redis-files";
  CommandProcessor processor(database, false, nullptr, &config);

  redis::CommandResult result = processor.Execute({"CONFIG", "GET", "dir"});
  Expect(result.has_value(), "CONFIG GET dir should succeed");
  Expect(std::holds_alternative<RespArray>(*result),
         "CONFIG GET dir should return a RESP array");

  const auto& response = std::get<RespArray>(*result);
  Expect(response.values ==
             std::vector<std::string>({"dir", "/tmp/redis-files"}),
         "CONFIG GET dir should return the configured dir");
  Expect(RespWriter::Write(*result) ==
             "*2\r\n$3\r\ndir\r\n$16\r\n/tmp/redis-files\r\n",
         "CONFIG GET dir should encode as the expected RESP array");
}

void TestConfigGetDbfilenameReturnsConfiguredFilename() {
  Database database;
  ServerConfig config;
  config.dbfilename = "dump.rdb";
  CommandProcessor processor(database, false, nullptr, &config);

  redis::CommandResult result =
      processor.Execute({"CONFIG", "GET", "dbfilename"});
  Expect(result.has_value(), "CONFIG GET dbfilename should succeed");
  Expect(std::holds_alternative<RespArray>(*result),
         "CONFIG GET dbfilename should return a RESP array");

  const auto& response = std::get<RespArray>(*result);
  Expect(response.values ==
             std::vector<std::string>({"dbfilename", "dump.rdb"}),
         "CONFIG GET dbfilename should return the configured filename");
  Expect(RespWriter::Write(*result) ==
             "*2\r\n$10\r\ndbfilename\r\n$8\r\ndump.rdb\r\n",
         "CONFIG GET dbfilename should encode as the expected RESP array");
}

void TestKeysReturnsStoredKeys() {
  Database database;
  database.SetString("foo", "123");
  CommandProcessor processor(database, false);

  redis::CommandResult result = processor.Execute({"KEYS", "*"});
  Expect(result.has_value(), "KEYS * should succeed");
  Expect(std::holds_alternative<RespArray>(*result),
         "KEYS * should return a RESP array");

  const auto& response = std::get<RespArray>(*result);
  Expect(response.values == std::vector<std::string>({"foo"}),
         "KEYS * should return the stored key");
  Expect(RespWriter::Write(*result) == "*1\r\n$3\r\nfoo\r\n",
         "KEYS * should encode the key as a RESP array");
}

void TestRdbLoaderImportsSingleStringKey() {
  Database database;
  ServerConfig config;

  char directory_template[] = "/tmp/redis-rdb-testXXXXXX";
  char* directory = mkdtemp(directory_template);
  Expect(directory != nullptr, "mkdtemp should succeed");

  config.dir = directory;
  config.dbfilename = "dump.rdb";
  const std::filesystem::path file_path =
      std::filesystem::path(config.dir) / config.dbfilename;

  const std::vector<unsigned char> rdb_bytes = {
      'R',  'E',  'D',  'I',  'S',  '0',  '0',  '1',  '1',
      0xFE, 0x00, 0xFB, 0x01, 0x00, 0x00, 0x03, 'f',  'o',
      'o',  0x03, 'b',  'a',  'r',  0xFF, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00,
  };
  {
    std::ofstream output(file_path, std::ios::binary);
    output.write(reinterpret_cast<const char*>(rdb_bytes.data()),
                 static_cast<std::streamsize>(rdb_bytes.size()));
  }

  LoadDatabaseFromRdb(config, database);
  CommandProcessor processor(database, false);

  redis::CommandResult result = processor.Execute({"KEYS", "*"});
  Expect(result.has_value(), "KEYS * should succeed after loading RDB");
  Expect(std::holds_alternative<RespArray>(*result),
         "KEYS * should return a RESP array after loading RDB");
  Expect(std::get<RespArray>(*result).values ==
             std::vector<std::string>({"foo"}),
         "RDB loader should import the single key from the RDB file");

  std::filesystem::remove(file_path);
  std::filesystem::remove(config.dir);
}

}  // namespace

int main() {
  TestReplicaReplconfGetackReturnsAck();
  TestMasterReplconfStillReturnsOk();
  TestWaitReturnsZeroImmediatelyWithoutReplicas();
  TestWaitReturnsConnectedReplicaCount();
  TestWaitBlocksUntilReplicaAcknowledgesPreviousWrite();
  TestConfigGetDirReturnsConfiguredDirectory();
  TestConfigGetDbfilenameReturnsConfiguredFilename();
  TestKeysReturnsStoredKeys();
  TestRdbLoaderImportsSingleStringKey();
  return 0;
}
