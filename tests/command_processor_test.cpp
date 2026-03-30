#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <cerrno>
#include <memory>
#include <thread>
#include <sys/socket.h>
#include <unistd.h>
#include <string>
#include <vector>

#include "redis-cpp/client_session.hpp"
#include "redis-cpp/command_processor.hpp"
#include "redis-cpp/pubsub_manager.hpp"
#include "redis-cpp/replica_manager.hpp"
#include "redis-cpp/rdb_loader.hpp"
#include "redis-cpp/unique_fd.hpp"

namespace {

using redis::ClientSession;
using redis::CommandProcessor;
using redis::Database;
using redis::PubSubManager;
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

void TestSubscribeReturnsConfirmationFrame() {
  Database database;
  CommandProcessor processor(database, false);

  redis::CommandResult result = processor.Execute({"SUBSCRIBE", "foo"});
  Expect(result.has_value(), "SUBSCRIBE foo should succeed");
  Expect(std::holds_alternative<redis::RespRaw>(*result),
         "SUBSCRIBE foo should return a raw RESP frame");
  Expect(RespWriter::Write(*result) ==
             "*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n",
         "SUBSCRIBE foo should encode the expected confirmation array");
}

void TestZaddCreatesSortedSetAndReturnsAddedCount() {
  Database database;
  CommandProcessor processor(database, false);

  redis::CommandResult result =
      processor.Execute({"ZADD", "zset_key", "10.0", "zset_member"});
  Expect(result.has_value(), "ZADD should succeed");
  Expect(std::holds_alternative<RespInteger>(*result),
         "ZADD should return a RESP integer");
  Expect(std::get<RespInteger>(*result).value == 1,
         "ZADD should report one added member for a new sorted set");
  Expect(RespWriter::Write(*result) == ":1\r\n",
         "ZADD should encode the added-member count as a RESP integer");
}

void TestZrankReturnsSortedSetRankAndNilForMissingMembers() {
  Database database;
  CommandProcessor processor(database, false);

  Expect(processor.Execute({"ZADD", "zset_key", "100.0", "foo"}).has_value(),
         "setup ZADD foo should succeed");
  Expect(processor.Execute({"ZADD", "zset_key", "100.0", "bar"}).has_value(),
         "setup ZADD bar should succeed");
  Expect(processor.Execute({"ZADD", "zset_key", "20.0", "baz"}).has_value(),
         "setup ZADD baz should succeed");
  Expect(processor.Execute({"ZADD", "zset_key", "30.1", "caz"}).has_value(),
         "setup ZADD caz should succeed");
  Expect(processor.Execute({"ZADD", "zset_key", "40.2", "paz"}).has_value(),
         "setup ZADD paz should succeed");

  redis::CommandResult result = processor.Execute({"ZRANK", "zset_key", "caz"});
  Expect(result.has_value(), "ZRANK caz should succeed");
  Expect(std::holds_alternative<RespInteger>(*result),
         "ZRANK should return a RESP integer for existing members");
  Expect(std::get<RespInteger>(*result).value == 1,
         "ZRANK should return the member rank in score order");
  Expect(RespWriter::Write(*result) == ":1\r\n",
         "ZRANK should encode ranks as RESP integers");

  result = processor.Execute({"ZRANK", "zset_key", "bar"});
  Expect(result.has_value(), "ZRANK bar should succeed");
  Expect(std::holds_alternative<RespInteger>(*result),
         "ZRANK bar should return a RESP integer");
  Expect(std::get<RespInteger>(*result).value == 3,
         "ZRANK should break score ties lexicographically");

  result = processor.Execute({"ZRANK", "zset_key", "missing_member"});
  Expect(result.has_value(), "ZRANK missing member should succeed");
  Expect(std::holds_alternative<redis::RespNullBulk>(*result),
         "ZRANK missing member should return a null bulk string");
  Expect(RespWriter::Write(*result) == "$-1\r\n",
         "ZRANK missing member should encode as a null bulk string");

  result = processor.Execute({"ZRANK", "missing_key", "member"});
  Expect(result.has_value(), "ZRANK missing key should succeed");
  Expect(std::holds_alternative<redis::RespNullBulk>(*result),
         "ZRANK missing key should return a null bulk string");
}

void TestZrangeReturnsSortedSetMembersByIndex() {
  Database database;
  CommandProcessor processor(database, false);

  Expect(processor.Execute({"ZADD", "zset_key", "100.0", "foo"}).has_value(),
         "setup ZADD foo should succeed");
  Expect(processor.Execute({"ZADD", "zset_key", "100.0", "bar"}).has_value(),
         "setup ZADD bar should succeed");
  Expect(processor.Execute({"ZADD", "zset_key", "20.0", "baz"}).has_value(),
         "setup ZADD baz should succeed");
  Expect(processor.Execute({"ZADD", "zset_key", "30.1", "caz"}).has_value(),
         "setup ZADD caz should succeed");
  Expect(processor.Execute({"ZADD", "zset_key", "40.2", "paz"}).has_value(),
         "setup ZADD paz should succeed");

  redis::CommandResult result = processor.Execute({"ZRANGE", "zset_key", "2", "4"});
  Expect(result.has_value(), "ZRANGE 2 4 should succeed");
  Expect(std::holds_alternative<RespArray>(*result),
         "ZRANGE should return a RESP array");
  Expect(std::get<RespArray>(*result).values ==
             std::vector<std::string>({"paz", "bar", "foo"}),
         "ZRANGE should return members in sorted-set order");
  Expect(RespWriter::Write(*result) ==
             "*3\r\n$3\r\npaz\r\n$3\r\nbar\r\n$3\r\nfoo\r\n",
         "ZRANGE should encode members as a RESP array");

  result = processor.Execute({"ZRANGE", "zset_key", "0", "10"});
  Expect(result.has_value(), "ZRANGE 0 10 should succeed");
  Expect(std::holds_alternative<RespArray>(*result),
         "ZRANGE with large stop should return a RESP array");
  Expect(std::get<RespArray>(*result).values ==
             std::vector<std::string>({"baz", "caz", "paz", "bar", "foo"}),
         "ZRANGE should clamp stop to the last member");

  result = processor.Execute({"ZRANGE", "zset_key", "2", "-1"});
  Expect(result.has_value(), "ZRANGE 2 -1 should succeed");
  Expect(std::holds_alternative<RespArray>(*result),
         "ZRANGE with a negative stop should return a RESP array");
  Expect(std::get<RespArray>(*result).values ==
             std::vector<std::string>({"paz", "bar", "foo"}),
         "ZRANGE should resolve -1 to the last member");

  result = processor.Execute({"ZRANGE", "zset_key", "0", "-3"});
  Expect(result.has_value(), "ZRANGE 0 -3 should succeed");
  Expect(std::holds_alternative<RespArray>(*result),
         "ZRANGE with a negative stop offset should return a RESP array");
  Expect(std::get<RespArray>(*result).values ==
             std::vector<std::string>({"baz", "caz", "paz"}),
         "ZRANGE should count negative indexes from the end");

  result = processor.Execute({"ZRANGE", "zset_key", "-2", "-1"});
  Expect(result.has_value(), "ZRANGE -2 -1 should succeed");
  Expect(std::holds_alternative<RespArray>(*result),
         "ZRANGE with negative start and stop should return a RESP array");
  Expect(std::get<RespArray>(*result).values ==
             std::vector<std::string>({"bar", "foo"}),
         "ZRANGE should resolve negative indexes from the tail");

  result = processor.Execute({"ZRANGE", "zset_key", "-99", "-1"});
  Expect(result.has_value(), "ZRANGE with out-of-range negative start should succeed");
  Expect(std::holds_alternative<RespArray>(*result),
         "ZRANGE with out-of-range negative start should return a RESP array");
  Expect(std::get<RespArray>(*result).values ==
             std::vector<std::string>({"baz", "caz", "paz", "bar", "foo"}),
         "ZRANGE should clamp out-of-range negative indexes to the start");

  result = processor.Execute({"ZRANGE", "zset_key", "5", "6"});
  Expect(result.has_value(), "ZRANGE past end should succeed");
  Expect(std::holds_alternative<RespArray>(*result),
         "ZRANGE past end should still return a RESP array");
  Expect(std::get<RespArray>(*result).values.empty(),
         "ZRANGE past end should return an empty array");
  Expect(RespWriter::Write(*result) == "*0\r\n",
         "ZRANGE empty results should encode as an empty array");

  result = processor.Execute({"ZRANGE", "zset_key", "4", "2"});
  Expect(result.has_value(), "ZRANGE with start > stop should succeed");
  Expect(std::holds_alternative<RespArray>(*result),
         "ZRANGE with start > stop should return a RESP array");
  Expect(std::get<RespArray>(*result).values.empty(),
         "ZRANGE with start > stop should return an empty array");

  result = processor.Execute({"ZRANGE", "missing_key", "0", "1"});
  Expect(result.has_value(), "ZRANGE missing key should succeed");
  Expect(std::holds_alternative<RespArray>(*result),
         "ZRANGE missing key should return a RESP array");
  Expect(std::get<RespArray>(*result).values.empty(),
         "ZRANGE missing key should return an empty array");
}

void TestZcardReturnsSortedSetCardinality() {
  Database database;
  CommandProcessor processor(database, false);

  Expect(processor.Execute({"ZADD", "zset_key", "20.0", "zset_member1"}).has_value(),
         "setup ZADD zset_member1 should succeed");
  Expect(processor.Execute({"ZADD", "zset_key", "30.1", "zset_member2"}).has_value(),
         "setup ZADD zset_member2 should succeed");
  Expect(processor.Execute({"ZADD", "zset_key", "40.2", "zset_member3"}).has_value(),
         "setup ZADD zset_member3 should succeed");
  Expect(processor.Execute({"ZADD", "zset_key", "50.3", "zset_member4"}).has_value(),
         "setup ZADD zset_member4 should succeed");

  redis::CommandResult result = processor.Execute({"ZCARD", "zset_key"});
  Expect(result.has_value(), "ZCARD should succeed");
  Expect(std::holds_alternative<RespInteger>(*result),
         "ZCARD should return a RESP integer");
  Expect(std::get<RespInteger>(*result).value == 4,
         "ZCARD should return the number of members in the sorted set");
  Expect(RespWriter::Write(*result) == ":4\r\n",
         "ZCARD should encode the cardinality as a RESP integer");

  result = processor.Execute({"ZADD", "zset_key", "100.0", "zset_member1"});
  Expect(result.has_value(), "updating an existing member should succeed");
  Expect(std::holds_alternative<RespInteger>(*result),
         "updating an existing member should return a RESP integer");
  Expect(std::get<RespInteger>(*result).value == 0,
         "updating an existing member should not change cardinality");

  result = processor.Execute({"ZCARD", "zset_key"});
  Expect(result.has_value(), "ZCARD after update should succeed");
  Expect(std::holds_alternative<RespInteger>(*result),
         "ZCARD after update should return a RESP integer");
  Expect(std::get<RespInteger>(*result).value == 4,
         "ZCARD should remain unchanged after updating an existing member");

  result = processor.Execute({"ZCARD", "missing_key"});
  Expect(result.has_value(), "ZCARD missing key should succeed");
  Expect(std::holds_alternative<RespInteger>(*result),
         "ZCARD missing key should return a RESP integer");
  Expect(std::get<RespInteger>(*result).value == 0,
         "ZCARD missing key should return zero");
}

void TestSubscribeTracksChannelsPerClientSession() {
  Database database;
  CommandProcessor processor(database, false);

  auto run_client = [&](const std::vector<std::string>& commands) {
    int fds[2];
    const int socket_pair_status = socketpair(AF_UNIX, SOCK_STREAM, 0, fds);
    Expect(socket_pair_status == 0, "socketpair should succeed");

    ClientSession session(redis::Socket(redis::UniqueFd(fds[0])), processor,
                          nullptr);
    std::thread session_thread([&]() { session.Run(); });

    std::vector<std::string> responses;
    char buffer[256];
    for (const std::string& command : commands) {
      const ssize_t sent = send(fds[1], command.data(), command.size(), 0);
      Expect(sent == static_cast<ssize_t>(command.size()),
             "test command should be written fully to the client socket");

      const ssize_t received = recv(fds[1], buffer, sizeof(buffer), 0);
      Expect(received > 0, "client session should send a subscribe response");
      responses.emplace_back(buffer, buffer + received);
    }

    close(fds[1]);
    session_thread.join();
    return responses;
  };

  const auto first_client_responses = run_client({
      "*2\r\n$9\r\nSUBSCRIBE\r\n$3\r\nfoo\r\n",
      "*2\r\n$9\r\nSUBSCRIBE\r\n$3\r\nbar\r\n",
      "*2\r\n$9\r\nSUBSCRIBE\r\n$3\r\nbar\r\n",
  });
  Expect(first_client_responses.size() == 3,
         "first client should receive three subscribe responses");
  Expect(first_client_responses[0] ==
             "*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n",
         "first subscribe should report one subscribed channel");
  Expect(first_client_responses[1] ==
             "*3\r\n$9\r\nsubscribe\r\n$3\r\nbar\r\n:2\r\n",
         "second distinct subscribe should report two subscribed channels");
  Expect(first_client_responses[2] ==
             "*3\r\n$9\r\nsubscribe\r\n$3\r\nbar\r\n:2\r\n",
         "duplicate subscribe should keep the subscribed channel count");

  const auto second_client_responses = run_client({
      "*2\r\n$9\r\nSUBSCRIBE\r\n$3\r\nbaz\r\n",
  });
  Expect(second_client_responses.size() == 1,
         "second client should receive one subscribe response");
  Expect(second_client_responses[0] ==
             "*3\r\n$9\r\nsubscribe\r\n$3\r\nbaz\r\n:1\r\n",
         "subscribed channel counts should be maintained per client");
}

void TestSubscribedModeRejectsDisallowedCommands() {
  Database database;
  CommandProcessor processor(database, false);

  int fds[2];
  const int socket_pair_status = socketpair(AF_UNIX, SOCK_STREAM, 0, fds);
  Expect(socket_pair_status == 0, "socketpair should succeed");

  ClientSession session(redis::Socket(redis::UniqueFd(fds[0])), processor,
                        nullptr);
  std::thread session_thread([&]() { session.Run(); });

  char buffer[256];

  const std::string subscribe_foo =
      "*2\r\n$9\r\nSUBSCRIBE\r\n$3\r\nfoo\r\n";
  Expect(send(fds[1], subscribe_foo.data(), subscribe_foo.size(), 0) ==
             static_cast<ssize_t>(subscribe_foo.size()),
         "initial SUBSCRIBE should be written fully");
  ssize_t received = recv(fds[1], buffer, sizeof(buffer), 0);
  Expect(received > 0, "initial SUBSCRIBE should receive a response");
  Expect(std::string(buffer, buffer + received) ==
             "*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n",
         "initial SUBSCRIBE should enter subscribed mode");

  const std::string echo_hey =
      "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n";
  Expect(send(fds[1], echo_hey.data(), echo_hey.size(), 0) ==
             static_cast<ssize_t>(echo_hey.size()),
         "disallowed command should be written fully");
  received = recv(fds[1], buffer, sizeof(buffer), 0);
  Expect(received > 0, "disallowed command should receive an error");
  const std::string error_response(buffer, buffer + received);
  Expect(error_response.starts_with("-ERR Can't execute 'ECHO'"),
         "subscribed mode should reject non-pubsub commands");

  const std::string subscribe_bar =
      "*2\r\n$9\r\nSUBSCRIBE\r\n$3\r\nbar\r\n";
  Expect(send(fds[1], subscribe_bar.data(), subscribe_bar.size(), 0) ==
             static_cast<ssize_t>(subscribe_bar.size()),
         "follow-up SUBSCRIBE should be written fully");
  received = recv(fds[1], buffer, sizeof(buffer), 0);
  Expect(received > 0, "follow-up SUBSCRIBE should receive a response");
  Expect(std::string(buffer, buffer + received) ==
             "*3\r\n$9\r\nsubscribe\r\n$3\r\nbar\r\n:2\r\n",
         "SUBSCRIBE should still be allowed in subscribed mode");

  close(fds[1]);
  session_thread.join();
}

void TestSubscribedModePingUsesPubsubResponse() {
  Database database;
  CommandProcessor processor(database, false);

  auto run_client = [&](const std::vector<std::string>& commands) {
    int fds[2];
    const int socket_pair_status = socketpair(AF_UNIX, SOCK_STREAM, 0, fds);
    Expect(socket_pair_status == 0, "socketpair should succeed");

    ClientSession session(redis::Socket(redis::UniqueFd(fds[0])), processor,
                          nullptr);
    std::thread session_thread([&]() { session.Run(); });

    std::vector<std::string> responses;
    char buffer[256];
    for (const std::string& command : commands) {
      Expect(send(fds[1], command.data(), command.size(), 0) ==
                 static_cast<ssize_t>(command.size()),
             "test command should be written fully to the client socket");
      const ssize_t received = recv(fds[1], buffer, sizeof(buffer), 0);
      Expect(received > 0, "client session should send a response");
      responses.emplace_back(buffer, buffer + received);
    }

    close(fds[1]);
    session_thread.join();
    return responses;
  };

  const auto subscribed_responses = run_client({
      "*2\r\n$9\r\nSUBSCRIBE\r\n$3\r\nfoo\r\n",
      "*1\r\n$4\r\nPING\r\n",
  });
  Expect(subscribed_responses.size() == 2,
         "subscribed client should receive two responses");
  Expect(subscribed_responses[1] == "*2\r\n$4\r\npong\r\n$0\r\n\r\n",
         "PING in subscribed mode should use the pubsub response");

  const auto regular_responses = run_client({
      "*1\r\n$4\r\nPING\r\n",
  });
  Expect(regular_responses.size() == 1,
         "regular client should receive one response");
  Expect(regular_responses[0] == "+PONG\r\n",
         "PING outside subscribed mode should keep the normal response");
}

void TestPublishReturnsSubscribedClientCount() {
  Database database;
  CommandProcessor processor(database, false);
  PubSubManager pubsub_manager;

  auto start_client = [&](const std::vector<std::string>& initial_commands) {
    int fds[2];
    const int socket_pair_status = socketpair(AF_UNIX, SOCK_STREAM, 0, fds);
    Expect(socket_pair_status == 0, "socketpair should succeed");

    auto session = std::make_unique<ClientSession>(
        redis::Socket(redis::UniqueFd(fds[0])), processor, nullptr,
        &pubsub_manager);
    std::thread session_thread([session = session.get()]() { session->Run(); });

    char buffer[256];
    for (const std::string& command : initial_commands) {
      Expect(send(fds[1], command.data(), command.size(), 0) ==
                 static_cast<ssize_t>(command.size()),
             "initial client command should be written fully");
      const ssize_t received = recv(fds[1], buffer, sizeof(buffer), 0);
      Expect(received > 0, "initial client command should receive a response");
    }

    struct RunningClient {
      int fd = -1;
      std::unique_ptr<ClientSession> session;
      std::thread thread;
    };

    return RunningClient{
        .fd = fds[1],
        .session = std::move(session),
        .thread = std::move(session_thread),
    };
  };

  auto subscriber_foo = start_client({
      "*2\r\n$9\r\nSUBSCRIBE\r\n$3\r\nfoo\r\n",
  });
  auto subscriber_bar_one = start_client({
      "*2\r\n$9\r\nSUBSCRIBE\r\n$3\r\nbar\r\n",
  });
  auto subscriber_bar_two = start_client({
      "*2\r\n$9\r\nSUBSCRIBE\r\n$3\r\nbar\r\n",
  });
  auto publisher = start_client({});

  char buffer[256];

  const std::string publish_bar =
      "*3\r\n$7\r\nPUBLISH\r\n$3\r\nbar\r\n$3\r\nmsg\r\n";
  Expect(send(publisher.fd, publish_bar.data(), publish_bar.size(), 0) ==
             static_cast<ssize_t>(publish_bar.size()),
         "PUBLISH bar should be written fully");
  ssize_t received = recv(publisher.fd, buffer, sizeof(buffer), 0);
  Expect(received > 0, "PUBLISH bar should receive a response");
  Expect(std::string(buffer, buffer + received) == ":2\r\n",
         "PUBLISH bar should report two subscribed clients");

  received = recv(subscriber_bar_one.fd, buffer, sizeof(buffer), 0);
  Expect(received > 0, "first bar subscriber should receive the published message");
  Expect(std::string(buffer, buffer + received) ==
             "*3\r\n$7\r\nmessage\r\n$3\r\nbar\r\n$3\r\nmsg\r\n",
         "first bar subscriber should receive the bar message frame");

  received = recv(subscriber_bar_two.fd, buffer, sizeof(buffer), 0);
  Expect(received > 0, "second bar subscriber should receive the published message");
  Expect(std::string(buffer, buffer + received) ==
             "*3\r\n$7\r\nmessage\r\n$3\r\nbar\r\n$3\r\nmsg\r\n",
         "second bar subscriber should receive the bar message frame");

  errno = 0;
  received = recv(subscriber_foo.fd, buffer, sizeof(buffer), MSG_DONTWAIT);
  Expect(received < 0 && (errno == EAGAIN || errno == EWOULDBLOCK),
         "foo subscriber should not receive bar messages");

  const std::string publish_foo =
      "*3\r\n$7\r\nPUBLISH\r\n$3\r\nfoo\r\n$3\r\nmsg\r\n";
  Expect(send(publisher.fd, publish_foo.data(), publish_foo.size(), 0) ==
             static_cast<ssize_t>(publish_foo.size()),
         "PUBLISH foo should be written fully");
  received = recv(publisher.fd, buffer, sizeof(buffer), 0);
  Expect(received > 0, "PUBLISH foo should receive a response");
  Expect(std::string(buffer, buffer + received) == ":1\r\n",
         "PUBLISH foo should report one subscribed client");

  received = recv(subscriber_foo.fd, buffer, sizeof(buffer), 0);
  Expect(received > 0, "foo subscriber should receive the foo message");
  Expect(std::string(buffer, buffer + received) ==
             "*3\r\n$7\r\nmessage\r\n$3\r\nfoo\r\n$3\r\nmsg\r\n",
         "foo subscriber should receive the foo message frame");

  auto stop_client = [](auto& client) {
    close(client.fd);
    client.thread.join();
  };

  stop_client(subscriber_foo);
  stop_client(subscriber_bar_one);
  stop_client(subscriber_bar_two);
  stop_client(publisher);
}

void TestUnsubscribeRemovesChannelAndStopsDelivery() {
  Database database;
  CommandProcessor processor(database, false);
  PubSubManager pubsub_manager;

  auto start_client = [&](const std::vector<std::string>& initial_commands) {
    int fds[2];
    const int socket_pair_status = socketpair(AF_UNIX, SOCK_STREAM, 0, fds);
    Expect(socket_pair_status == 0, "socketpair should succeed");

    auto session = std::make_unique<ClientSession>(
        redis::Socket(redis::UniqueFd(fds[0])), processor, nullptr,
        &pubsub_manager);
    std::thread session_thread([session = session.get()]() { session->Run(); });

    char buffer[256];
    for (const std::string& command : initial_commands) {
      Expect(send(fds[1], command.data(), command.size(), 0) ==
                 static_cast<ssize_t>(command.size()),
             "initial client command should be written fully");
      const ssize_t received = recv(fds[1], buffer, sizeof(buffer), 0);
      Expect(received > 0, "initial client command should receive a response");
    }

    struct RunningClient {
      int fd = -1;
      std::unique_ptr<ClientSession> session;
      std::thread thread;
    };

    return RunningClient{
        .fd = fds[1],
        .session = std::move(session),
        .thread = std::move(session_thread),
    };
  };

  auto subscriber = start_client({
      "*2\r\n$9\r\nSUBSCRIBE\r\n$3\r\nfoo\r\n",
      "*2\r\n$9\r\nSUBSCRIBE\r\n$3\r\nbaz\r\n",
  });
  auto publisher = start_client({});

  char buffer[256];

  const std::string publish_before =
      "*3\r\n$7\r\nPUBLISH\r\n$3\r\nfoo\r\n$18\r\nbefore-unsubscribe\r\n";
  Expect(send(publisher.fd, publish_before.data(), publish_before.size(), 0) ==
             static_cast<ssize_t>(publish_before.size()),
         "initial publish should be written fully");
  ssize_t received = recv(publisher.fd, buffer, sizeof(buffer), 0);
  Expect(received > 0, "initial publish should receive a response");
  Expect(std::string(buffer, buffer + received) == ":1\r\n",
         "initial publish should report one subscriber");

  received = recv(subscriber.fd, buffer, sizeof(buffer), 0);
  Expect(received > 0, "subscriber should receive the first published message");
  Expect(std::string(buffer, buffer + received) ==
             "*3\r\n$7\r\nmessage\r\n$3\r\nfoo\r\n$18\r\nbefore-unsubscribe\r\n",
         "subscriber should receive the initial foo message");

  const std::string unsubscribe_unknown =
      "*2\r\n$11\r\nUNSUBSCRIBE\r\n$3\r\nbar\r\n";
  Expect(send(subscriber.fd, unsubscribe_unknown.data(),
              unsubscribe_unknown.size(), 0) ==
             static_cast<ssize_t>(unsubscribe_unknown.size()),
         "UNSUBSCRIBE unknown channel should be written fully");
  received = recv(subscriber.fd, buffer, sizeof(buffer), 0);
  Expect(received > 0, "UNSUBSCRIBE unknown channel should receive a response");
  Expect(std::string(buffer, buffer + received) ==
             "*3\r\n$11\r\nunsubscribe\r\n$3\r\nbar\r\n:2\r\n",
         "UNSUBSCRIBE unknown channel should keep the subscribed count");

  const std::string unsubscribe_foo =
      "*2\r\n$11\r\nUNSUBSCRIBE\r\n$3\r\nfoo\r\n";
  Expect(send(subscriber.fd, unsubscribe_foo.data(), unsubscribe_foo.size(), 0) ==
             static_cast<ssize_t>(unsubscribe_foo.size()),
         "UNSUBSCRIBE foo should be written fully");
  received = recv(subscriber.fd, buffer, sizeof(buffer), 0);
  Expect(received > 0, "UNSUBSCRIBE foo should receive a response");
  Expect(std::string(buffer, buffer + received) ==
             "*3\r\n$11\r\nunsubscribe\r\n$3\r\nfoo\r\n:1\r\n",
         "UNSUBSCRIBE foo should reduce the subscribed count");

  const std::string publish_after =
      "*3\r\n$7\r\nPUBLISH\r\n$3\r\nfoo\r\n$17\r\nafter-unsubscribe\r\n";
  Expect(send(publisher.fd, publish_after.data(), publish_after.size(), 0) ==
             static_cast<ssize_t>(publish_after.size()),
         "publish after unsubscribe should be written fully");
  received = recv(publisher.fd, buffer, sizeof(buffer), 0);
  Expect(received > 0, "publish after unsubscribe should receive a response");
  Expect(std::string(buffer, buffer + received) == ":0\r\n",
         "publish after unsubscribe should report zero subscribers");

  errno = 0;
  received = recv(subscriber.fd, buffer, sizeof(buffer), MSG_DONTWAIT);
  Expect(received < 0 && (errno == EAGAIN || errno == EWOULDBLOCK),
         "client should not receive messages for unsubscribed channels");

  auto stop_client = [](auto& client) {
    close(client.fd);
    client.thread.join();
  };

  stop_client(subscriber);
  stop_client(publisher);
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

void TestRdbLoaderMakesLoadedValueAvailableToGet() {
  Database database;
  ServerConfig config;

  char directory_template[] = "/tmp/redis-rdb-get-testXXXXXX";
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

  redis::CommandResult result = processor.Execute({"GET", "foo"});
  Expect(result.has_value(), "GET foo should succeed after loading RDB");
  Expect(std::holds_alternative<redis::RespBulkString>(*result),
         "GET foo should return a RESP bulk string after loading RDB");
  Expect(std::get<redis::RespBulkString>(*result).value == "bar",
         "GET foo should return the loaded value from the RDB file");
  Expect(RespWriter::Write(*result) == "$3\r\nbar\r\n",
         "GET foo should encode the loaded value as a RESP bulk string");

  std::filesystem::remove(file_path);
  std::filesystem::remove(config.dir);
}

void TestRdbLoaderImportsMultipleStringValues() {
  Database database;
  ServerConfig config;

  char directory_template[] = "/tmp/redis-rdb-multi-testXXXXXX";
  char* directory = mkdtemp(directory_template);
  Expect(directory != nullptr, "mkdtemp should succeed");

  config.dir = directory;
  config.dbfilename = "dump.rdb";
  const std::filesystem::path file_path =
      std::filesystem::path(config.dir) / config.dbfilename;

  const std::vector<unsigned char> rdb_bytes = {
      'R',  'E',  'D',  'I',  'S',  '0',  '0',  '1',  '1',
      0xFE, 0x00, 0xFB, 0x02, 0x00,
      0x00, 0x03, 'f',  'o',  'o',  0x03, 'o',  'n',  'e',
      0x00, 0x03, 'b',  'a',  'r',  0x03, 't',  'w',  'o',
      0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
  };
  {
    std::ofstream output(file_path, std::ios::binary);
    output.write(reinterpret_cast<const char*>(rdb_bytes.data()),
                 static_cast<std::streamsize>(rdb_bytes.size()));
  }

  LoadDatabaseFromRdb(config, database);
  CommandProcessor processor(database, false);

  redis::CommandResult foo_result = processor.Execute({"GET", "foo"});
  Expect(foo_result.has_value(), "GET foo should succeed after loading RDB");
  Expect(std::holds_alternative<redis::RespBulkString>(*foo_result),
         "GET foo should return a RESP bulk string");
  Expect(std::get<redis::RespBulkString>(*foo_result).value == "one",
         "GET foo should return the first loaded value");

  redis::CommandResult bar_result = processor.Execute({"GET", "bar"});
  Expect(bar_result.has_value(), "GET bar should succeed after loading RDB");
  Expect(std::holds_alternative<redis::RespBulkString>(*bar_result),
         "GET bar should return a RESP bulk string");
  Expect(std::get<redis::RespBulkString>(*bar_result).value == "two",
         "GET bar should return the second loaded value");

  std::filesystem::remove(file_path);
  std::filesystem::remove(config.dir);
}

void AppendLittleEndian64(std::vector<unsigned char>& bytes, uint64_t value) {
  for (int shift = 0; shift < 64; shift += 8) {
    bytes.push_back(static_cast<unsigned char>((value >> shift) & 0xFF));
  }
}

void TestRdbLoaderRespectsExpiredAndLiveKeys() {
  Database database;
  ServerConfig config;

  char directory_template[] = "/tmp/redis-rdb-expiry-testXXXXXX";
  char* directory = mkdtemp(directory_template);
  Expect(directory != nullptr, "mkdtemp should succeed");

  config.dir = directory;
  config.dbfilename = "dump.rdb";
  const std::filesystem::path file_path =
      std::filesystem::path(config.dir) / config.dbfilename;

  const auto now = std::chrono::time_point_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now());
  const uint64_t now_ms =
      static_cast<uint64_t>(now.time_since_epoch().count());
  const uint64_t expired_ms = now_ms - 60000;
  const uint64_t live_ms = now_ms + 60000;

  std::vector<unsigned char> rdb_bytes = {
      'R',  'E',  'D',  'I',  'S',  '0',  '0',  '1',  '1',
      0xFE, 0x00, 0xFB, 0x02, 0x01,
      0xFC,
  };
  AppendLittleEndian64(rdb_bytes, expired_ms);
  rdb_bytes.insert(rdb_bytes.end(),
                   {0x00, 0x03, 'f', 'o', 'o', 0x03, 'o', 'l', 'd'});
  rdb_bytes.push_back(0xFC);
  AppendLittleEndian64(rdb_bytes, live_ms);
  rdb_bytes.insert(rdb_bytes.end(),
                   {0x00, 0x03, 'b', 'a', 'r', 0x03, 'n', 'e', 'w',
                    0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                    0x00});

  {
    std::ofstream output(file_path, std::ios::binary);
    output.write(reinterpret_cast<const char*>(rdb_bytes.data()),
                 static_cast<std::streamsize>(rdb_bytes.size()));
  }

  LoadDatabaseFromRdb(config, database);
  CommandProcessor processor(database, false);

  redis::CommandResult expired_result = processor.Execute({"GET", "foo"});
  Expect(expired_result.has_value(), "GET foo should succeed after loading RDB");
  Expect(std::holds_alternative<redis::RespNullBulk>(*expired_result),
         "GET foo should return a null bulk string for expired data");
  Expect(RespWriter::Write(*expired_result) == "$-1\r\n",
         "Expired keys loaded from RDB should encode as null bulk strings");

  redis::CommandResult live_result = processor.Execute({"GET", "bar"});
  Expect(live_result.has_value(), "GET bar should succeed after loading RDB");
  Expect(std::holds_alternative<redis::RespBulkString>(*live_result),
         "GET bar should return a RESP bulk string for live data");
  Expect(std::get<redis::RespBulkString>(*live_result).value == "new",
         "GET bar should return the non-expired RDB value");

  std::filesystem::remove(file_path);
  std::filesystem::remove(config.dir);
}

}  // namespace

int main() {
  TestReplicaReplconfGetackReturnsAck();
  TestMasterReplconfStillReturnsOk();
  TestSubscribeReturnsConfirmationFrame();
  TestZaddCreatesSortedSetAndReturnsAddedCount();
  TestZrankReturnsSortedSetRankAndNilForMissingMembers();
  TestZrangeReturnsSortedSetMembersByIndex();
  TestZcardReturnsSortedSetCardinality();
  TestSubscribeTracksChannelsPerClientSession();
  TestSubscribedModeRejectsDisallowedCommands();
  TestSubscribedModePingUsesPubsubResponse();
  TestPublishReturnsSubscribedClientCount();
  TestUnsubscribeRemovesChannelAndStopsDelivery();
  TestWaitReturnsZeroImmediatelyWithoutReplicas();
  TestWaitReturnsConnectedReplicaCount();
  TestWaitBlocksUntilReplicaAcknowledgesPreviousWrite();
  TestConfigGetDirReturnsConfiguredDirectory();
  TestConfigGetDbfilenameReturnsConfiguredFilename();
  TestKeysReturnsStoredKeys();
  TestRdbLoaderImportsSingleStringKey();
  TestRdbLoaderMakesLoadedValueAvailableToGet();
  TestRdbLoaderImportsMultipleStringValues();
  TestRdbLoaderRespectsExpiredAndLiveKeys();
  return 0;
}
