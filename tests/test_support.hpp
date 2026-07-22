#pragma once

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>
#include <sys/socket.h>
#include <unistd.h>

#include "redis-cpp/client_session.hpp"
#include "redis-cpp/command_executor.hpp"
#include "redis-cpp/pubsub_manager.hpp"
#include "redis-cpp/rdb_loader.hpp"
#include "redis-cpp/replica_manager.hpp"
#include "redis-cpp/resp.hpp"
#include "redis-cpp/unique_fd.hpp"

inline std::vector<std::string> RespBulkStrings(
    const redis::RespArray& array) {
  std::vector<std::string> values;
  values.reserve(array.values.size());
  for (const redis::RespValue& value : array.values) {
    if (!value.Is<redis::RespBulkString>()) {
      ADD_FAILURE() << "expected a RESP bulk string array";
      return {};
    }
    values.push_back(value.Get<redis::RespBulkString>().value);
  }
  return values;
}
