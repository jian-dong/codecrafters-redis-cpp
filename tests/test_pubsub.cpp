#include "test_support.hpp"

namespace {

using redis::ClientSession;
using redis::CommandExecutor;
using redis::Database;
using redis::PubSubManager;
using redis::RespArray;
using redis::RespInteger;
using redis::RespSimpleString;
using redis::RespWriter;

TEST(PubSubTest, SubscribeReturnsConfirmationFrame) {
  Database database;
  CommandExecutor executor(database, false);

  redis::CommandResult result = executor.Execute({"SUBSCRIBE", "foo"});
  ASSERT_TRUE((result.has_value())) << "SUBSCRIBE foo should succeed";
  ASSERT_TRUE((std::holds_alternative<redis::RespRaw>(*result))) << "SUBSCRIBE foo should return a raw RESP frame";
  ASSERT_TRUE((RespWriter::Write(*result) ==
             "*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n")) << "SUBSCRIBE foo should encode the expected confirmation array";
}

TEST(PubSubTest, SubscribeTracksChannelsPerClientSession) {
  Database database;
  CommandExecutor executor(database, false);

  auto run_client = [&](const std::vector<std::string>& commands) {
    int fds[2];
    const int socket_pair_status = socketpair(AF_UNIX, SOCK_STREAM, 0, fds);
    EXPECT_TRUE((socket_pair_status == 0)) << "socketpair should succeed";

    ClientSession session(redis::Socket(redis::UniqueFd(fds[0])), executor,
                          nullptr);
    std::thread session_thread([&]() { session.Run(); });

    std::vector<std::string> responses;
    char buffer[256];
    for (const std::string& command : commands) {
      const ssize_t sent = send(fds[1], command.data(), command.size(), 0);
      EXPECT_TRUE((sent == static_cast<ssize_t>(command.size()))) << "test command should be written fully to the client socket";

      const ssize_t received = recv(fds[1], buffer, sizeof(buffer), 0);
      EXPECT_TRUE((received > 0)) << "client session should send a subscribe response";
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
  ASSERT_TRUE((first_client_responses.size() == 3)) << "first client should receive three subscribe responses";
  ASSERT_TRUE((first_client_responses[0] ==
             "*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n")) << "first subscribe should report one subscribed channel";
  ASSERT_TRUE((first_client_responses[1] ==
             "*3\r\n$9\r\nsubscribe\r\n$3\r\nbar\r\n:2\r\n")) << "second distinct subscribe should report two subscribed channels";
  ASSERT_TRUE((first_client_responses[2] ==
             "*3\r\n$9\r\nsubscribe\r\n$3\r\nbar\r\n:2\r\n")) << "duplicate subscribe should keep the subscribed channel count";

  const auto second_client_responses = run_client({
      "*2\r\n$9\r\nSUBSCRIBE\r\n$3\r\nbaz\r\n",
  });
  ASSERT_TRUE((second_client_responses.size() == 1)) << "second client should receive one subscribe response";
  ASSERT_TRUE((second_client_responses[0] ==
             "*3\r\n$9\r\nsubscribe\r\n$3\r\nbaz\r\n:1\r\n")) << "subscribed channel counts should be maintained per client";
}

TEST(PubSubTest, SubscribedModeRejectsDisallowedCommands) {
  Database database;
  CommandExecutor executor(database, false);

  int fds[2];
  const int socket_pair_status = socketpair(AF_UNIX, SOCK_STREAM, 0, fds);
  ASSERT_TRUE((socket_pair_status == 0)) << "socketpair should succeed";

  ClientSession session(redis::Socket(redis::UniqueFd(fds[0])), executor,
                        nullptr);
  std::thread session_thread([&]() { session.Run(); });

  char buffer[256];

  const std::string subscribe_foo = "*2\r\n$9\r\nSUBSCRIBE\r\n$3\r\nfoo\r\n";
  ASSERT_TRUE((send(fds[1], subscribe_foo.data(), subscribe_foo.size(), 0) ==
             static_cast<ssize_t>(subscribe_foo.size()))) << "initial SUBSCRIBE should be written fully";
  ssize_t received = recv(fds[1], buffer, sizeof(buffer), 0);
  ASSERT_TRUE((received > 0)) << "initial SUBSCRIBE should receive a response";
  ASSERT_TRUE((std::string(buffer, buffer + received) ==
             "*3\r\n$9\r\nsubscribe\r\n$3\r\nfoo\r\n:1\r\n")) << "initial SUBSCRIBE should enter subscribed mode";

  const std::string echo_hey = "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n";
  ASSERT_TRUE((send(fds[1], echo_hey.data(), echo_hey.size(), 0) ==
             static_cast<ssize_t>(echo_hey.size()))) << "disallowed command should be written fully";
  received = recv(fds[1], buffer, sizeof(buffer), 0);
  ASSERT_TRUE((received > 0)) << "disallowed command should receive an error";
  const std::string error_response(buffer, buffer + received);
  ASSERT_TRUE((error_response.starts_with("-ERR Can't execute 'ECHO'"))) << "subscribed mode should reject non-pubsub commands";

  const std::string subscribe_bar = "*2\r\n$9\r\nSUBSCRIBE\r\n$3\r\nbar\r\n";
  ASSERT_TRUE((send(fds[1], subscribe_bar.data(), subscribe_bar.size(), 0) ==
             static_cast<ssize_t>(subscribe_bar.size()))) << "follow-up SUBSCRIBE should be written fully";
  received = recv(fds[1], buffer, sizeof(buffer), 0);
  ASSERT_TRUE((received > 0)) << "follow-up SUBSCRIBE should receive a response";
  ASSERT_TRUE((std::string(buffer, buffer + received) ==
             "*3\r\n$9\r\nsubscribe\r\n$3\r\nbar\r\n:2\r\n")) << "SUBSCRIBE should still be allowed in subscribed mode";

  close(fds[1]);
  session_thread.join();
}

TEST(PubSubTest, SubscribedModePingUsesPubsubResponse) {
  Database database;
  CommandExecutor executor(database, false);

  auto run_client = [&](const std::vector<std::string>& commands) {
    int fds[2];
    const int socket_pair_status = socketpair(AF_UNIX, SOCK_STREAM, 0, fds);
    EXPECT_TRUE((socket_pair_status == 0)) << "socketpair should succeed";

    ClientSession session(redis::Socket(redis::UniqueFd(fds[0])), executor,
                          nullptr);
    std::thread session_thread([&]() { session.Run(); });

    std::vector<std::string> responses;
    char buffer[256];
    for (const std::string& command : commands) {
      EXPECT_TRUE((send(fds[1], command.data(), command.size(), 0) ==
                 static_cast<ssize_t>(command.size()))) << "test command should be written fully to the client socket";
      const ssize_t received = recv(fds[1], buffer, sizeof(buffer), 0);
      EXPECT_TRUE((received > 0)) << "client session should send a response";
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
  ASSERT_TRUE((subscribed_responses.size() == 2)) << "subscribed client should receive two responses";
  ASSERT_TRUE((subscribed_responses[1] == "*2\r\n$4\r\npong\r\n$0\r\n\r\n")) << "PING in subscribed mode should use the pubsub response";

  const auto regular_responses = run_client({
      "*1\r\n$4\r\nPING\r\n",
  });
  ASSERT_TRUE((regular_responses.size() == 1)) << "regular client should receive one response";
  ASSERT_TRUE((regular_responses[0] == "+PONG\r\n")) << "PING outside subscribed mode should keep the normal response";
}

TEST(PubSubTest, PublishReturnsSubscribedClientCount) {
  Database database;
  CommandExecutor executor(database, false);
  PubSubManager pubsub_manager;

  auto start_client = [&](const std::vector<std::string>& initial_commands) {
    int fds[2];
    const int socket_pair_status = socketpair(AF_UNIX, SOCK_STREAM, 0, fds);
    EXPECT_TRUE((socket_pair_status == 0)) << "socketpair should succeed";

    auto session = std::make_unique<ClientSession>(
        redis::Socket(redis::UniqueFd(fds[0])), executor, nullptr,
        &pubsub_manager);
    std::thread session_thread([session = session.get()]() { session->Run(); });

    char buffer[256];
    for (const std::string& command : initial_commands) {
      EXPECT_TRUE((send(fds[1], command.data(), command.size(), 0) ==
                 static_cast<ssize_t>(command.size()))) << "initial client command should be written fully";
      const ssize_t received = recv(fds[1], buffer, sizeof(buffer), 0);
      EXPECT_TRUE((received > 0)) << "initial client command should receive a response";
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
  ASSERT_TRUE((send(publisher.fd, publish_bar.data(), publish_bar.size(), 0) ==
             static_cast<ssize_t>(publish_bar.size()))) << "PUBLISH bar should be written fully";
  ssize_t received = recv(publisher.fd, buffer, sizeof(buffer), 0);
  ASSERT_TRUE((received > 0)) << "PUBLISH bar should receive a response";
  ASSERT_TRUE((std::string(buffer, buffer + received) == ":2\r\n")) << "PUBLISH bar should report two subscribed clients";

  received = recv(subscriber_bar_one.fd, buffer, sizeof(buffer), 0);
  ASSERT_TRUE((received > 0)) << "first bar subscriber should receive the published message";
  ASSERT_TRUE((std::string(buffer, buffer + received) ==
             "*3\r\n$7\r\nmessage\r\n$3\r\nbar\r\n$3\r\nmsg\r\n")) << "first bar subscriber should receive the bar message frame";

  received = recv(subscriber_bar_two.fd, buffer, sizeof(buffer), 0);
  ASSERT_TRUE((received > 0)) << "second bar subscriber should receive the published message";
  ASSERT_TRUE((std::string(buffer, buffer + received) ==
             "*3\r\n$7\r\nmessage\r\n$3\r\nbar\r\n$3\r\nmsg\r\n")) << "second bar subscriber should receive the bar message frame";

  errno = 0;
  received = recv(subscriber_foo.fd, buffer, sizeof(buffer), MSG_DONTWAIT);
  ASSERT_TRUE((received < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))) << "foo subscriber should not receive bar messages";

  const std::string publish_foo =
      "*3\r\n$7\r\nPUBLISH\r\n$3\r\nfoo\r\n$3\r\nmsg\r\n";
  ASSERT_TRUE((send(publisher.fd, publish_foo.data(), publish_foo.size(), 0) ==
             static_cast<ssize_t>(publish_foo.size()))) << "PUBLISH foo should be written fully";
  received = recv(publisher.fd, buffer, sizeof(buffer), 0);
  ASSERT_TRUE((received > 0)) << "PUBLISH foo should receive a response";
  ASSERT_TRUE((std::string(buffer, buffer + received) == ":1\r\n")) << "PUBLISH foo should report one subscribed client";

  received = recv(subscriber_foo.fd, buffer, sizeof(buffer), 0);
  ASSERT_TRUE((received > 0)) << "foo subscriber should receive the foo message";
  ASSERT_TRUE((std::string(buffer, buffer + received) ==
             "*3\r\n$7\r\nmessage\r\n$3\r\nfoo\r\n$3\r\nmsg\r\n")) << "foo subscriber should receive the foo message frame";

  auto stop_client = [](auto& client) {
    close(client.fd);
    client.thread.join();
  };

  stop_client(subscriber_foo);
  stop_client(subscriber_bar_one);
  stop_client(subscriber_bar_two);
  stop_client(publisher);
}

TEST(PubSubTest, UnsubscribeRemovesChannelAndStopsDelivery) {
  Database database;
  CommandExecutor executor(database, false);
  PubSubManager pubsub_manager;

  auto start_client = [&](const std::vector<std::string>& initial_commands) {
    int fds[2];
    const int socket_pair_status = socketpair(AF_UNIX, SOCK_STREAM, 0, fds);
    EXPECT_TRUE((socket_pair_status == 0)) << "socketpair should succeed";

    auto session = std::make_unique<ClientSession>(
        redis::Socket(redis::UniqueFd(fds[0])), executor, nullptr,
        &pubsub_manager);
    std::thread session_thread([session = session.get()]() { session->Run(); });

    char buffer[256];
    for (const std::string& command : initial_commands) {
      EXPECT_TRUE((send(fds[1], command.data(), command.size(), 0) ==
                 static_cast<ssize_t>(command.size()))) << "initial client command should be written fully";
      const ssize_t received = recv(fds[1], buffer, sizeof(buffer), 0);
      EXPECT_TRUE((received > 0)) << "initial client command should receive a response";
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
  ASSERT_TRUE((send(publisher.fd, publish_before.data(), publish_before.size(), 0) ==
             static_cast<ssize_t>(publish_before.size()))) << "initial publish should be written fully";
  ssize_t received = recv(publisher.fd, buffer, sizeof(buffer), 0);
  ASSERT_TRUE((received > 0)) << "initial publish should receive a response";
  ASSERT_TRUE((std::string(buffer, buffer + received) == ":1\r\n")) << "initial publish should report one subscriber";

  received = recv(subscriber.fd, buffer, sizeof(buffer), 0);
  ASSERT_TRUE((received > 0)) << "subscriber should receive the first published message";
  ASSERT_TRUE((std::string(buffer, buffer + received) ==
             "*3\r\n$7\r\nmessage\r\n$3\r\nfoo\r\n$18\r\nbefore-unsubscribe\r\n")) << "subscriber should receive the initial foo message";

  const std::string unsubscribe_unknown =
      "*2\r\n$11\r\nUNSUBSCRIBE\r\n$3\r\nbar\r\n";
  ASSERT_TRUE((send(subscriber.fd, unsubscribe_unknown.data(),
              unsubscribe_unknown.size(), 0) ==
             static_cast<ssize_t>(unsubscribe_unknown.size()))) << "UNSUBSCRIBE unknown channel should be written fully";
  received = recv(subscriber.fd, buffer, sizeof(buffer), 0);
  ASSERT_TRUE((received > 0)) << "UNSUBSCRIBE unknown channel should receive a response";
  ASSERT_TRUE((std::string(buffer, buffer + received) ==
             "*3\r\n$11\r\nunsubscribe\r\n$3\r\nbar\r\n:2\r\n")) << "UNSUBSCRIBE unknown channel should keep the subscribed count";

  const std::string unsubscribe_foo =
      "*2\r\n$11\r\nUNSUBSCRIBE\r\n$3\r\nfoo\r\n";
  ASSERT_TRUE((send(subscriber.fd, unsubscribe_foo.data(), unsubscribe_foo.size(), 0) ==
             static_cast<ssize_t>(unsubscribe_foo.size()))) << "UNSUBSCRIBE foo should be written fully";
  received = recv(subscriber.fd, buffer, sizeof(buffer), 0);
  ASSERT_TRUE((received > 0)) << "UNSUBSCRIBE foo should receive a response";
  ASSERT_TRUE((std::string(buffer, buffer + received) ==
             "*3\r\n$11\r\nunsubscribe\r\n$3\r\nfoo\r\n:1\r\n")) << "UNSUBSCRIBE foo should reduce the subscribed count";

  const std::string publish_after =
      "*3\r\n$7\r\nPUBLISH\r\n$3\r\nfoo\r\n$17\r\nafter-unsubscribe\r\n";
  ASSERT_TRUE((send(publisher.fd, publish_after.data(), publish_after.size(), 0) ==
             static_cast<ssize_t>(publish_after.size()))) << "publish after unsubscribe should be written fully";
  received = recv(publisher.fd, buffer, sizeof(buffer), 0);
  ASSERT_TRUE((received > 0)) << "publish after unsubscribe should receive a response";
  ASSERT_TRUE((std::string(buffer, buffer + received) == ":0\r\n")) << "publish after unsubscribe should report zero subscribers";

  errno = 0;
  received = recv(subscriber.fd, buffer, sizeof(buffer), MSG_DONTWAIT);
  ASSERT_TRUE((received < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))) << "client should not receive messages for unsubscribed channels";

  auto stop_client = [](auto& client) {
    close(client.fd);
    client.thread.join();
  };

  stop_client(subscriber);
  stop_client(publisher);
}

}  // namespace
