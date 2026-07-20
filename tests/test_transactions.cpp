#include "test_support.hpp"

namespace {

using redis::CommandErrorCode;
using redis::ClientSession;
using redis::CommandExecutor;
using redis::Database;
using redis::RespSimpleString;
using redis::RespWriter;

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

TEST(TransactionTest, WatchRequiresExactlyOneKey) {
  Database database;
  CommandExecutor executor(database);

  redis::CommandResult missing_key = executor.Execute({"WATCH"});
  ASSERT_FALSE(missing_key.has_value());
  EXPECT_EQ(missing_key.error().code, CommandErrorCode::kWrongArity);

  redis::CommandResult multiple_keys =
      executor.Execute({"WATCH", "key1", "key2"});
  ASSERT_FALSE(multiple_keys.has_value());
  EXPECT_EQ(multiple_keys.error().code, CommandErrorCode::kWrongArity);
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

}  // namespace
