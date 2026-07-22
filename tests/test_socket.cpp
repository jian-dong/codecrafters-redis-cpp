#include "test_support.hpp"

#include <future>
#include <memory>
#include <string>

#include "redis-cpp/socket.hpp"

namespace {

TEST(ConnectionWriterTest, RejectsWritesAfterClose) {
  int sockets[2];
  ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
  redis::ConnectionWriter writer(sockets[0]);

  writer.Close();
  const redis::Status result = writer.SendAll("ignored");

  ASSERT_FALSE(result.has_value());
  const auto* error =
      std::get_if<redis::NetworkError>(&result.error().Kind());
  ASSERT_NE(error, nullptr);
  EXPECT_EQ(error->code, redis::NetworkErrorCode::kConnectionClosed);
  char byte = '\0';
  EXPECT_EQ(recv(sockets[1], &byte, 1, MSG_DONTWAIT), -1);
  close(sockets[0]);
  close(sockets[1]);
}

TEST(ConnectionWriterTest, SerializesConcurrentFrames) {
  int sockets[2];
  ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sockets), 0);
  auto writer = std::make_shared<redis::ConnectionWriter>(sockets[0]);

  const std::string first(128 * 1024, 'a');
  const std::string second(128 * 1024, 'b');
  auto receiver = std::async(std::launch::async, [&]() {
    std::string received(first.size() + second.size(), '\0');
    size_t offset = 0;
    while (offset < received.size()) {
      const ssize_t count = recv(sockets[1], received.data() + offset,
                                 received.size() - offset, 0);
      if (count <= 0) {
        break;
      }
      offset += static_cast<size_t>(count);
    }
    received.resize(offset);
    return received;
  });

  auto first_send =
      std::async(std::launch::async, [&]() { return writer->SendAll(first); });
  auto second_send = std::async(std::launch::async,
                                [&]() { return writer->SendAll(second); });

  ASSERT_TRUE(first_send.get().has_value());
  ASSERT_TRUE(second_send.get().has_value());
  const std::string received = receiver.get();
  EXPECT_TRUE(received == first + second || received == second + first);

  writer->Close();
  close(sockets[0]);
  close(sockets[1]);
}

}  // namespace
