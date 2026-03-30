#include "test_support.hpp"

namespace {

using redis::ClientSession;
using redis::CommandExecutor;
using redis::Database;
using redis::RespSimpleString;
using redis::RespWriter;

TEST(AuthTest, ValidatesPasswordAgainstAclHashes) {
  Database database;
  CommandExecutor executor(database, false);

  ASSERT_TRUE((executor.Execute({"ACL", "SETUSER", "default", ">mypassword"}).has_value())) << "setup ACL SETUSER default >mypassword should succeed";

  redis::CommandResult result =
      executor.Execute({"AUTH", "default", "wrongpassword"});
  ASSERT_TRUE((!result.has_value())) << "AUTH with a wrong password should fail";
  const std::string wrongpass_error =
      RespWriter::Error(CommandErrorMessage(result.error()));
  ASSERT_TRUE((wrongpass_error.starts_with("-WRONGPASS"))) << "AUTH with a wrong password should return a WRONGPASS error";

  result = executor.Execute({"AUTH", "default", "mypassword"});
  ASSERT_TRUE((result.has_value())) << "AUTH with the correct password should succeed";
  ASSERT_TRUE((std::holds_alternative<RespSimpleString>(*result))) << "AUTH with the correct password should return a RESP simple string";
  ASSERT_TRUE((std::get<RespSimpleString>(*result).value == "OK")) << "AUTH with the correct password should return OK";
  ASSERT_TRUE((RespWriter::Write(*result) == "+OK\r\n")) << "AUTH with the correct password should encode OK as a RESP simple string";
}

TEST(AuthTest, NewConnectionsRequireAuthAfterDefaultPasswordIsSet) {
  Database database;
  CommandExecutor executor(database, false);

  int authenticated_fds[2];
  ASSERT_TRUE((socketpair(AF_UNIX, SOCK_STREAM, 0, authenticated_fds) == 0)) << "authenticated socketpair should succeed";
  ClientSession authenticated_session(
      redis::Socket(redis::UniqueFd(authenticated_fds[0])), executor, nullptr);
  std::thread authenticated_thread([&]() { authenticated_session.Run(); });

  char buffer[512];
  const std::string setuser_command =
      "*4\r\n$3\r\nACL\r\n$7\r\nSETUSER\r\n$7\r\ndefault\r\n$12\r\n>newpassword\r\n";
  ASSERT_TRUE((send(authenticated_fds[1], setuser_command.data(), setuser_command.size(), 0) ==
             static_cast<ssize_t>(setuser_command.size()))) << "ACL SETUSER should be written fully on the authenticated connection";
  ssize_t received = recv(authenticated_fds[1], buffer, sizeof(buffer), 0);
  ASSERT_TRUE((received > 0)) << "ACL SETUSER should receive a response";
  ASSERT_TRUE((std::string(buffer, buffer + received) == "+OK\r\n")) << "ACL SETUSER should return OK on the authenticated connection";

  const std::string whoami_command = "*2\r\n$3\r\nACL\r\n$6\r\nWHOAMI\r\n";
  ASSERT_TRUE((send(authenticated_fds[1], whoami_command.data(), whoami_command.size(), 0) ==
             static_cast<ssize_t>(whoami_command.size()))) << "ACL WHOAMI should be written fully on the authenticated connection";
  received = recv(authenticated_fds[1], buffer, sizeof(buffer), 0);
  ASSERT_TRUE((received > 0)) << "ACL WHOAMI should receive a response";
  ASSERT_TRUE((std::string(buffer, buffer + received) == "$7\r\ndefault\r\n")) << "existing authenticated connections should remain authenticated";

  int unauthenticated_fds[2];
  ASSERT_TRUE((socketpair(AF_UNIX, SOCK_STREAM, 0, unauthenticated_fds) == 0)) << "unauthenticated socketpair should succeed";
  ClientSession unauthenticated_session(
      redis::Socket(redis::UniqueFd(unauthenticated_fds[0])), executor, nullptr);
  std::thread unauthenticated_thread([&]() { unauthenticated_session.Run(); });

  ASSERT_TRUE((send(unauthenticated_fds[1], whoami_command.data(), whoami_command.size(), 0) ==
             static_cast<ssize_t>(whoami_command.size()))) << "ACL WHOAMI should be written fully on the unauthenticated connection";
  received = recv(unauthenticated_fds[1], buffer, sizeof(buffer), 0);
  ASSERT_TRUE((received > 0)) << "unauthenticated ACL WHOAMI should receive a response";
  ASSERT_TRUE((std::string(buffer, buffer + received).starts_with("-NOAUTH"))) << "new connections should receive NOAUTH after the default password is set";

  close(authenticated_fds[1]);
  authenticated_thread.join();
  close(unauthenticated_fds[1]);
  unauthenticated_thread.join();
}

TEST(AuthTest, AuthenticatesConnectionForSubsequentCommands) {
  Database database;
  CommandExecutor executor(database, false);

  int authenticated_fds[2];
  ASSERT_TRUE((socketpair(AF_UNIX, SOCK_STREAM, 0, authenticated_fds) == 0)) << "authenticated socketpair should succeed";
  ClientSession authenticated_session(
      redis::Socket(redis::UniqueFd(authenticated_fds[0])), executor, nullptr);
  std::thread authenticated_thread([&]() { authenticated_session.Run(); });

  char buffer[512];
  const std::string setuser_command =
      "*4\r\n$3\r\nACL\r\n$7\r\nSETUSER\r\n$7\r\ndefault\r\n$12\r\n>newpassword\r\n";
  ASSERT_TRUE((send(authenticated_fds[1], setuser_command.data(), setuser_command.size(), 0) ==
             static_cast<ssize_t>(setuser_command.size()))) << "ACL SETUSER should be written fully on the authenticated connection";
  ssize_t received = recv(authenticated_fds[1], buffer, sizeof(buffer), 0);
  ASSERT_TRUE((received > 0)) << "ACL SETUSER should receive a response";
  ASSERT_TRUE((std::string(buffer, buffer + received) == "+OK\r\n")) << "ACL SETUSER should succeed on the authenticated connection";

  int unauthenticated_fds[2];
  ASSERT_TRUE((socketpair(AF_UNIX, SOCK_STREAM, 0, unauthenticated_fds) == 0)) << "unauthenticated socketpair should succeed";
  ClientSession unauthenticated_session(
      redis::Socket(redis::UniqueFd(unauthenticated_fds[0])), executor, nullptr);
  std::thread unauthenticated_thread([&]() { unauthenticated_session.Run(); });

  const std::string ping_command = "*1\r\n$4\r\nPING\r\n";
  ASSERT_TRUE((send(unauthenticated_fds[1], ping_command.data(), ping_command.size(), 0) ==
             static_cast<ssize_t>(ping_command.size()))) << "PING should be written fully on the unauthenticated connection";
  received = recv(unauthenticated_fds[1], buffer, sizeof(buffer), 0);
  ASSERT_TRUE((received > 0)) << "unauthenticated PING should receive a response";
  ASSERT_TRUE((std::string(buffer, buffer + received).starts_with("-NOAUTH"))) << "unauthenticated PING should return NOAUTH";

  const std::string auth_command =
      "*3\r\n$4\r\nAUTH\r\n$7\r\ndefault\r\n$11\r\nnewpassword\r\n";
  ASSERT_TRUE((send(unauthenticated_fds[1], auth_command.data(), auth_command.size(), 0) ==
             static_cast<ssize_t>(auth_command.size()))) << "AUTH should be written fully on the unauthenticated connection";
  received = recv(unauthenticated_fds[1], buffer, sizeof(buffer), 0);
  ASSERT_TRUE((received > 0)) << "AUTH should receive a response";
  ASSERT_TRUE((std::string(buffer, buffer + received) == "+OK\r\n")) << "AUTH should succeed with the correct password";

  ASSERT_TRUE((send(unauthenticated_fds[1], ping_command.data(), ping_command.size(), 0) ==
             static_cast<ssize_t>(ping_command.size()))) << "PING should be written fully after AUTH";
  received = recv(unauthenticated_fds[1], buffer, sizeof(buffer), 0);
  ASSERT_TRUE((received > 0)) << "authenticated PING should receive a response";
  ASSERT_TRUE((std::string(buffer, buffer + received) == "+PONG\r\n")) << "PING should succeed after AUTH authenticates the connection";

  close(authenticated_fds[1]);
  authenticated_thread.join();
  close(unauthenticated_fds[1]);
  unauthenticated_thread.join();
}

}  // namespace
