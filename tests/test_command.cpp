#include "test_support.hpp"

#include "redis-cpp/command.hpp"

namespace {

TEST(CommandSemanticsTest, ClassifiesCommandsCaseInsensitively) {
  EXPECT_EQ(redis::DescribeCommand("set").id, redis::CommandId::kSet);
  EXPECT_EQ(redis::DescribeCommand("GeT").id, redis::CommandId::kGet);
  EXPECT_EQ(redis::DescribeCommand("missing").id,
            redis::CommandId::kUnknown);
}

TEST(CommandSemanticsTest, CentralizesWriteAndTransactionProperties) {
  const redis::CommandSemantics set = redis::DescribeCommand("SET");
  const redis::CommandSemantics zrem = redis::DescribeCommand("ZREM");
  const redis::CommandSemantics get = redis::DescribeCommand("GET");
  const redis::CommandSemantics blpop = redis::DescribeCommand("BLPOP");

  EXPECT_TRUE(set.may_write);
  EXPECT_TRUE(set.serialized_with_transactions);
  EXPECT_TRUE(zrem.may_write);
  EXPECT_TRUE(zrem.serialized_with_transactions);
  EXPECT_FALSE(get.may_write);
  EXPECT_TRUE(get.serialized_with_transactions)
      << "reads must not observe a partially executed transaction";
  EXPECT_TRUE(blpop.may_write);
  EXPECT_FALSE(blpop.serialized_with_transactions)
      << "blocking commands must not hold the transaction lock while waiting";
}

TEST(CommandSemanticsTest, CentralizesSubscribedModePolicy) {
  EXPECT_TRUE(redis::DescribeCommand("PING").allowed_while_subscribed);
  EXPECT_TRUE(redis::DescribeCommand("UNSUBSCRIBE").allowed_while_subscribed);
  EXPECT_FALSE(redis::DescribeCommand("GET").allowed_while_subscribed);
}

TEST(CommandSemanticsTest, OmitsEffectsForSuccessfulNoOps) {
  const auto effects = redis::DescribeCommandEffects(
      redis::DescribeCommand("BLPOP"), {"BLPOP", "items", "0.01"},
      redis::RespNullArray{}, 1000);

  EXPECT_FALSE(effects.persistence.has_value());
  EXPECT_FALSE(effects.replication.has_value());
}

TEST(CommandSemanticsTest, PublishReplicatesWithoutBeingPersisted) {
  const std::vector<std::string> command = {"PUBLISH", "events", "hello"};

  const auto effects = redis::DescribeCommandEffects(
      redis::DescribeCommand("PUBLISH"), command, redis::RespInteger{0},
      1000);

  EXPECT_FALSE(effects.persistence.has_value());
  ASSERT_TRUE(effects.replication.has_value());
  EXPECT_EQ(*effects.replication, command);
}

TEST(CommandSemanticsTest, CanonicalizesGeneratedBlockingAndRelativeEffects) {
  const auto xadd = redis::DescribeCommandEffects(
      redis::DescribeCommand("XADD"),
      {"XADD", "events", "*", "field", "value"},
      redis::RespBulkString{"123-4"}, 1000);
  ASSERT_TRUE(xadd.persistence.has_value());
  EXPECT_EQ(*xadd.persistence,
            (std::vector<std::string>{"XADD", "events", "123-4", "field",
                                      "value"}));

  const auto blpop = redis::DescribeCommandEffects(
      redis::DescribeCommand("BLPOP"), {"BLPOP", "items", "0"},
      redis::RespArray::BulkStrings({"items", "value"}), 1000);
  ASSERT_TRUE(blpop.persistence.has_value());
  EXPECT_EQ(*blpop.persistence,
            (std::vector<std::string>{"LPOP", "items"}));

  const auto set = redis::DescribeCommandEffects(
      redis::DescribeCommand("SET"), {"SET", "key", "value", "PX", "250"},
      redis::RespSimpleString{"OK"}, 1000);
  ASSERT_TRUE(set.persistence.has_value());
  EXPECT_EQ(*set.persistence,
            (std::vector<std::string>{"SET", "key", "value", "PXAT",
                                      "1250"}));
}

}  // namespace
