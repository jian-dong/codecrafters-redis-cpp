#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#include "redis-cpp/command_processor.hpp"

namespace {

using redis::CommandProcessor;
using redis::Database;
using redis::RespArray;
using redis::RespSimpleString;
using redis::RespWriter;

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

}  // namespace

int main() {
  TestReplicaReplconfGetackReturnsAck();
  TestMasterReplconfStillReturnsOk();
  return 0;
}
