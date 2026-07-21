#include "test_support.hpp"

namespace {

using redis::CommandErrorCode;
using redis::ClientSession;
using redis::CommandExecutor;
using redis::Database;
using redis::RespSimpleString;
using redis::RespWriter;

std::string ExchangeCommand(int fd, const std::vector<std::string>& args) {
  std::string encoded = "*" + std::to_string(args.size()) + "\r\n";
  for (const std::string& arg : args) {
    encoded += "$" + std::to_string(arg.size()) + "\r\n" + arg + "\r\n";
  }

  EXPECT_EQ(send(fd, encoded.data(), encoded.size(), 0),
            static_cast<ssize_t>(encoded.size()));
  char buffer[512];
  const ssize_t received = recv(fd, buffer, sizeof(buffer), 0);
  EXPECT_GT(received, 0);
  return received > 0 ? std::string(buffer, buffer + received) : std::string{};
}

TEST(TransactionTest, WatchSingleKeyReturnsOk) {
  Database database;
  CommandExecutor executor(database);

  redis::CommandResult result = executor.Execute({"WATCH", "key"});

  ASSERT_TRUE(result.has_value());
  ASSERT_TRUE(std::holds_alternative<RespSimpleString>(*result));
  EXPECT_EQ(std::get<RespSimpleString>(*result).value, "OK");
  EXPECT_EQ(RespWriter::Write(*result), "+OK\r\n");
}

TEST(TransactionTest, WatchCommandIsCaseInsensitive) {
  Database database;
  CommandExecutor executor(database);

  redis::CommandResult result = executor.Execute({"watch", "key"});

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(RespWriter::Write(*result), "+OK\r\n");
}

TEST(TransactionTest, WatchRequiresAtLeastOneKey) {
  Database database;
  CommandExecutor executor(database);

  redis::CommandResult missing_key = executor.Execute({"WATCH"});
  ASSERT_FALSE(missing_key.has_value());
  EXPECT_EQ(missing_key.error().code, CommandErrorCode::kWrongArity);

  redis::CommandResult multiple_keys =
      executor.Execute({"WATCH", "key1", "key2"});
  ASSERT_TRUE(multiple_keys.has_value());
  EXPECT_EQ(RespWriter::Write(*multiple_keys), "+OK\r\n");
}

TEST(TransactionTest, UnwatchReturnsOkAndRejectsArguments) {
  Database database;
  CommandExecutor executor(database);

  redis::CommandResult result = executor.Execute({"UNWATCH"});
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(RespWriter::Write(*result), "+OK\r\n");

  result = executor.Execute({"UNWATCH", "unexpected"});
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code, CommandErrorCode::kWrongArity);
}

TEST(TransactionTest, MultiReturnsOk) {
  Database database;
  CommandExecutor executor(database);

  redis::CommandResult result = executor.Execute({"MULTI"});

  ASSERT_TRUE(result.has_value());
  ASSERT_TRUE(std::holds_alternative<RespSimpleString>(*result));
  EXPECT_EQ(RespWriter::Write(*result), "+OK\r\n");
}

TEST(TransactionTest, MultiRejectsArguments) {
  Database database;
  CommandExecutor executor(database);

  redis::CommandResult result = executor.Execute({"MULTI", "unexpected"});

  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code, CommandErrorCode::kWrongArity);
}

TEST(TransactionTest, WatchInsideMultiReturnsError) {
  Database database;
  CommandExecutor executor(database);
  int fds[2];
  ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);

  ClientSession session(redis::Socket(redis::UniqueFd(fds[0])), executor);
  std::thread session_thread([&]() { session.Run(); });
  char buffer[256];

  const std::string watch_before_multi =
      "*2\r\n$5\r\nWATCH\r\n$7\r\ncounter\r\n";
  ASSERT_EQ(send(fds[1], watch_before_multi.data(), watch_before_multi.size(), 0),
            static_cast<ssize_t>(watch_before_multi.size()));
  ssize_t received = recv(fds[1], buffer, sizeof(buffer), 0);
  ASSERT_GT(received, 0);
  EXPECT_EQ(std::string(buffer, buffer + received), "+OK\r\n");

  const std::string multi = "*1\r\n$5\r\nMULTI\r\n";
  ASSERT_EQ(send(fds[1], multi.data(), multi.size(), 0),
            static_cast<ssize_t>(multi.size()));
  received = recv(fds[1], buffer, sizeof(buffer), 0);
  ASSERT_GT(received, 0);
  EXPECT_EQ(std::string(buffer, buffer + received), "+OK\r\n");

  const std::string watch_inside_multi =
      "*2\r\n$5\r\nWATCH\r\n$5\r\nother\r\n";
  ASSERT_EQ(send(fds[1], watch_inside_multi.data(), watch_inside_multi.size(), 0),
            static_cast<ssize_t>(watch_inside_multi.size()));
  received = recv(fds[1], buffer, sizeof(buffer), 0);
  ASSERT_GT(received, 0);
  const std::string error(buffer, buffer + received);
  EXPECT_TRUE(error.starts_with("-ERR"));
  EXPECT_NE(error.find("WATCH"), std::string::npos);
  EXPECT_NE(error.find("inside MULTI"), std::string::npos);
  EXPECT_NE(error.find("not allowed"), std::string::npos);

  close(fds[1]);
  session_thread.join();
}

TEST(TransactionTest, ExecAbortsWhenAnyWatchedKeyWasTouched) {
  Database database;
  CommandExecutor executor(database);
  int first_fds[2];
  int second_fds[2];
  ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, first_fds), 0);
  ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, second_fds), 0);

  ClientSession first_session(
      redis::Socket(redis::UniqueFd(first_fds[0])), executor);
  ClientSession second_session(
      redis::Socket(redis::UniqueFd(second_fds[0])), executor);
  std::thread first_thread([&]() { first_session.Run(); });
  std::thread second_thread([&]() { second_session.Run(); });

  EXPECT_EQ(ExchangeCommand(first_fds[1], {"SET", "foo", "100"}),
            "+OK\r\n");
  EXPECT_EQ(ExchangeCommand(first_fds[1], {"SET", "bar", "200"}),
            "+OK\r\n");
  EXPECT_EQ(ExchangeCommand(first_fds[1], {"WATCH", "foo", "bar"}),
            "+OK\r\n");
  EXPECT_EQ(ExchangeCommand(first_fds[1], {"MULTI"}), "+OK\r\n");
  EXPECT_EQ(ExchangeCommand(first_fds[1], {"SET", "bar", "300"}),
            "+QUEUED\r\n");

  EXPECT_EQ(ExchangeCommand(second_fds[1], {"SET", "foo", "999"}),
            "+OK\r\n");
  EXPECT_EQ(ExchangeCommand(second_fds[1], {"SET", "foo", "100"}),
            "+OK\r\n");

  EXPECT_EQ(ExchangeCommand(first_fds[1], {"EXEC"}), "*-1\r\n");
  EXPECT_EQ(ExchangeCommand(first_fds[1], {"GET", "bar"}),
            "$3\r\n200\r\n");

  close(first_fds[1]);
  close(second_fds[1]);
  first_thread.join();
  second_thread.join();
}

TEST(TransactionTest, UnwatchAllowsTransactionAfterWatchedKeyWasTouched) {
  Database database;
  CommandExecutor executor(database);
  int first_fds[2];
  int second_fds[2];
  ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, first_fds), 0);
  ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, second_fds), 0);

  ClientSession first_session(
      redis::Socket(redis::UniqueFd(first_fds[0])), executor);
  ClientSession second_session(
      redis::Socket(redis::UniqueFd(second_fds[0])), executor);
  std::thread first_thread([&]() { first_session.Run(); });
  std::thread second_thread([&]() { second_session.Run(); });

  EXPECT_EQ(ExchangeCommand(first_fds[1], {"SET", "baz", "100"}),
            "+OK\r\n");
  EXPECT_EQ(ExchangeCommand(first_fds[1], {"SET", "caz", "200"}),
            "+OK\r\n");
  EXPECT_EQ(ExchangeCommand(first_fds[1], {"WATCH", "baz", "caz"}),
            "+OK\r\n");
  EXPECT_EQ(ExchangeCommand(second_fds[1], {"SET", "baz", "300"}),
            "+OK\r\n");
  EXPECT_EQ(ExchangeCommand(first_fds[1], {"UNWATCH"}), "+OK\r\n");
  EXPECT_EQ(ExchangeCommand(first_fds[1], {"MULTI"}), "+OK\r\n");
  EXPECT_EQ(ExchangeCommand(first_fds[1], {"SET", "caz", "400"}),
            "+QUEUED\r\n");

  EXPECT_EQ(ExchangeCommand(first_fds[1], {"EXEC"}), "*1\r\n+OK\r\n");
  EXPECT_EQ(ExchangeCommand(second_fds[1], {"GET", "caz"}),
            "$3\r\n400\r\n");

  close(first_fds[1]);
  close(second_fds[1]);
  first_thread.join();
  second_thread.join();
}

}  // namespace
