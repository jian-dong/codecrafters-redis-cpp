#include "test_support.hpp"

namespace {

using redis::CommandExecutor;
using redis::Database;
using redis::RespSimpleString;
using redis::RespWriter;

TEST(AclTest, WhoamiReturnsDefaultUser) {
  Database database;
  CommandExecutor executor(database, false);

  redis::CommandResult result = executor.Execute({"ACL", "WHOAMI"});
  ASSERT_TRUE((result.has_value())) << "ACL WHOAMI should succeed";
  ASSERT_TRUE((std::holds_alternative<redis::RespBulkString>(*result))) << "ACL WHOAMI should return a RESP bulk string";
  ASSERT_TRUE((std::get<redis::RespBulkString>(*result).value == "default")) << "ACL WHOAMI should return the default user";
  ASSERT_TRUE((RespWriter::Write(*result) == "$7\r\ndefault\r\n")) << "ACL WHOAMI should encode the default user as a RESP bulk string";
}

TEST(AclTest, GetuserReturnsFlagsAndPasswordsForDefaultUser) {
  Database database;
  CommandExecutor executor(database, false);

  redis::CommandResult result = executor.Execute({"ACL", "GETUSER", "default"});
  ASSERT_TRUE((result.has_value())) << "ACL GETUSER default should succeed";
  ASSERT_TRUE((std::holds_alternative<redis::RespRaw>(*result))) << "ACL GETUSER default should return a raw RESP frame";
  ASSERT_TRUE((RespWriter::Write(*result) ==
             "*4\r\n$5\r\nflags\r\n*1\r\n$6\r\nnopass\r\n$9\r\npasswords\r\n*0\r\n")) << "ACL GETUSER default should return flags and empty passwords";
}

TEST(AclTest, SetuserStoresHashedPasswordAndClearsNopass) {
  Database database;
  CommandExecutor executor(database, false);

  redis::CommandResult result =
      executor.Execute({"ACL", "SETUSER", "default", ">mypassword"});
  ASSERT_TRUE((result.has_value())) << "ACL SETUSER default >mypassword should succeed";
  ASSERT_TRUE((std::holds_alternative<RespSimpleString>(*result))) << "ACL SETUSER should return a RESP simple string";
  ASSERT_TRUE((std::get<RespSimpleString>(*result).value == "OK")) << "ACL SETUSER should return OK";
  ASSERT_TRUE((RespWriter::Write(*result) == "+OK\r\n")) << "ACL SETUSER should encode OK as a RESP simple string";

  result = executor.Execute({"ACL", "GETUSER", "default"});
  ASSERT_TRUE((result.has_value())) << "ACL GETUSER default should succeed after setting a password";
  ASSERT_TRUE((std::holds_alternative<redis::RespRaw>(*result))) << "ACL GETUSER default should return a raw RESP frame after setting a password";
  ASSERT_TRUE((RespWriter::Write(*result) ==
          "*4\r\n$5\r\nflags\r\n*0\r\n$9\r\npasswords\r\n*1\r\n$64\r\n89e01536ac207279409d4de1e5253e01f4a1769e696db0d6062ca9b8f56767c8\r\n")) << "ACL GETUSER default should clear nopass and return the SHA-256 password hash";
}

}  // namespace
