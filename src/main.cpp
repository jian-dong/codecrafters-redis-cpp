#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <thread>
#include <climits>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <chrono>
#include <optional>
#include <cctype>
#include <cstdint>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>

constexpr int kServerPort = 6379;
constexpr int kServerBacklog = 5;
constexpr char kPongResponse[] = "+PONG\r\n";
constexpr char kOkResponse[] = "+OK\r\n";
constexpr char kNullBulkResponse[] = "$-1\r\n";

struct StoredValue {
  std::string value;
  std::optional<std::chrono::steady_clock::time_point> expires_at;
};

std::mutex gStoreMutex;
std::unordered_map<std::string, StoredValue> gStore;
std::mutex gListStoreMutex;
std::unordered_map<std::string, std::vector<std::string>> gListStore;

bool parse_number(const std::string& data, size_t& index, int& value) {
  value = 0;
  const size_t start = index;

  while (index < data.size() && data[index] >= '0' && data[index] <= '9') {
    const int digit = data[index] - '0';
    if (value > (INT_MAX - digit) / 10) {
      return false;
    }
    value = value * 10 + digit;
    ++index;
  }

  if (start == index) {
    return false;
  }

  if (index + 1 >= data.size() || data[index] != '\r' || data[index + 1] != '\n') {
    return false;
  }
  index += 2;
  return true;
}

bool parse_bulk_string(const std::string& data, size_t& index, std::string& value) {
  if (index >= data.size() || data[index] != '$') {
    return false;
  }

  ++index;
  int bulk_len = 0;
  if (!parse_number(data, index, bulk_len) || bulk_len < 0) {
    return false;
  }

  if (index + static_cast<size_t>(bulk_len) + 2 > data.size()) {
    return false;
  }

  value.assign(data.data() + index, static_cast<size_t>(bulk_len));
  index += static_cast<size_t>(bulk_len);
  if (index + 1 >= data.size() || data[index] != '\r' || data[index + 1] != '\n') {
    return false;
  }

  index += 2;
  return true;
}

bool parse_milliseconds(const std::string& data, int64_t& value) {
  if (data.empty()) {
    return false;
  }

  value = 0;
  for (const char ch : data) {
    if (ch < '0' || ch > '9') {
      return false;
    }

    const int digit = ch - '0';
    if (value > (LLONG_MAX - digit) / 10) {
      return false;
    }
    value = value * 10 + digit;
  }

  return true;
}

bool parse_signed_integer(const std::string& data, int64_t& value) {
  if (data.empty()) {
    return false;
  }

  bool negative = false;
  size_t index = 0;
  if (data[0] == '-') {
    negative = true;
    index = 1;
  }

  if (index == data.size()) {
    return false;
  }

  value = 0;
  for (; index < data.size(); ++index) {
    const char ch = data[index];
    if (ch < '0' || ch > '9') {
      return false;
    }

    const int digit = ch - '0';
    if (value > (LLONG_MAX - digit) / 10) {
      return false;
    }
    value = value * 10 + digit;
  }

  if (negative) {
    value = -value;
  }
  return true;
}

bool parse_resp_array(const std::string& data, size_t& index, std::vector<std::string>& args) {
  if (index >= data.size() || data[index] != '*') {
    return false;
  }

  ++index;
  int array_size = 0;
  if (!parse_number(data, index, array_size) || array_size < 0) {
    return false;
  }

  args.clear();
  args.reserve(array_size);
  for (int arg = 0; arg < array_size; ++arg) {
    std::string value;
    if (!parse_bulk_string(data, index, value)) {
      return false;
    }
    args.push_back(std::move(value));
  }

  return true;
}

std::string to_upper_ascii(std::string value) {
  for (char& ch : value) {
    ch = static_cast<char>(std::toupper(static_cast<unsigned char>(ch)));
  }
  return value;
}

void send_pong(int client_fd) {
  send(client_fd, kPongResponse, sizeof(kPongResponse) - 1, 0);
}

void send_ok(int client_fd) {
  send(client_fd, kOkResponse, sizeof(kOkResponse) - 1, 0);
}

void send_bulk_string(int client_fd, const std::string& value) {
  const std::string response = "$" + std::to_string(value.size()) + "\r\n" + value + "\r\n";
  send(client_fd, response.data(), response.size(), 0);
}

void send_null_bulk(int client_fd) {
  send(client_fd, kNullBulkResponse, sizeof(kNullBulkResponse) - 1, 0);
}

void send_integer(int client_fd, int64_t value) {
  const std::string response = ":" + std::to_string(value) + "\r\n";
  send(client_fd, response.data(), response.size(), 0);
}

void send_array(int client_fd, const std::vector<std::string>& values) {
  std::string response = "*" + std::to_string(values.size()) + "\r\n";
  for (const std::string& value : values) {
    response += "$" + std::to_string(value.size()) + "\r\n" + value + "\r\n";
  }
  send(client_fd, response.data(), response.size(), 0);
}

void handle_client(int client_fd) {
  std::string request_buffer;
  while (true) {
    char buffer[1024];
    const ssize_t bytes_received = recv(client_fd, buffer, sizeof(buffer), 0);
    if (bytes_received <= 0) {
      break;
    }

    request_buffer.append(buffer, bytes_received);

    while (!request_buffer.empty()) {
      size_t i = 0;
      if (request_buffer[0] == '*') {
        std::vector<std::string> args;
        if (!parse_resp_array(request_buffer, i, args)) {
          break;
        }
        request_buffer.erase(0, i);
        if (args.empty()) {
          send_pong(client_fd);
          continue;
        }

        const std::string command = to_upper_ascii(args[0]);
        if (command == "PING") {
          send_pong(client_fd);
          continue;
        }
        if (command == "ECHO" && args.size() >= 2) {
          send_bulk_string(client_fd, args[1]);
          continue;
        }
        if (command == "SET" && args.size() >= 3) {
          StoredValue entry{args[2], std::nullopt};
          if (args.size() >= 5 && to_upper_ascii(args[3]) == "PX") {
            int64_t ttl_milliseconds = 0;
            if (parse_milliseconds(args[4], ttl_milliseconds)) {
              entry.expires_at = std::chrono::steady_clock::now() + std::chrono::milliseconds(ttl_milliseconds);
            }
          }

          {
            std::lock_guard<std::mutex> lock(gStoreMutex);
            gStore[args[1]] = std::move(entry);
          }
          send_ok(client_fd);
          continue;
        }
        if (command == "GET" && args.size() >= 2) {
          std::string value;
          {
            std::lock_guard<std::mutex> lock(gStoreMutex);
            const auto found = gStore.find(args[1]);
            if (found == gStore.end()) {
              send_null_bulk(client_fd);
              continue;
            }

            const auto now = std::chrono::steady_clock::now();
            if (found->second.expires_at.has_value() && now >= *found->second.expires_at) {
              gStore.erase(found);
              send_null_bulk(client_fd);
              continue;
            }

            value = found->second.value;
          }
          send_bulk_string(client_fd, value);
          continue;
        }
        if (command == "RPUSH" && args.size() >= 3) {
          int64_t list_size = 0;
          {
            std::lock_guard<std::mutex> lock(gListStoreMutex);
            std::vector<std::string>& list = gListStore[args[1]];
            for (size_t arg_index = 2; arg_index < args.size(); ++arg_index) {
              list.push_back(args[arg_index]);
            }
            list_size = static_cast<int64_t>(list.size());
          }
          send_integer(client_fd, list_size);
          continue;
        }
        if (command == "LPUSH" && args.size() >= 3) {
          int64_t list_size = 0;
          {
            std::lock_guard<std::mutex> lock(gListStoreMutex);
            std::vector<std::string>& list = gListStore[args[1]];
            for (size_t arg_index = 2; arg_index < args.size(); ++arg_index) {
              list.insert(list.begin(), args[arg_index]);
            }
            list_size = static_cast<int64_t>(list.size());
          }
          send_integer(client_fd, list_size);
          continue;
        }
        if (command == "LRANGE" && args.size() >= 4) {
          int64_t start = 0;
          int64_t stop = 0;
          if (!parse_signed_integer(args[2], start) || !parse_signed_integer(args[3], stop)) {
            send_array(client_fd, {});
            continue;
          }

          std::vector<std::string> result;
          {
            std::lock_guard<std::mutex> lock(gListStoreMutex);
            const auto found = gListStore.find(args[1]);
            if (found != gListStore.end()) {
              const std::vector<std::string>& list = found->second;
              const int64_t list_size = static_cast<int64_t>(list.size());

              if (start < 0) {
                start += list_size;
              }
              if (stop < 0) {
                stop += list_size;
              }

              if (start < 0) {
                start = 0;
              }
              if (stop >= list_size) {
                stop = list_size - 1;
              }

              if (start < list_size && stop >= 0 && start <= stop) {
                result.reserve(static_cast<size_t>(stop - start + 1));
                for (int64_t index = start; index <= stop; ++index) {
                  result.push_back(list[static_cast<size_t>(index)]);
                }
              }
            }
          }

          send_array(client_fd, result);
          continue;
        }
        if (command == "LLEN" && args.size() >= 2) {
          int64_t list_size = 0;
          {
            std::lock_guard<std::mutex> lock(gListStoreMutex);
            const auto found = gListStore.find(args[1]);
            if (found != gListStore.end()) {
              list_size = static_cast<int64_t>(found->second.size());
            }
          }
          send_integer(client_fd, list_size);
          continue;
        }
        if (command == "LPOP" && args.size() >= 2) {
          std::string value;
          bool found_value = false;
          {
            std::lock_guard<std::mutex> lock(gListStoreMutex);
            const auto found = gListStore.find(args[1]);
            if (found != gListStore.end() && !found->second.empty()) {
              value = found->second.front();
              found->second.erase(found->second.begin());
              found_value = true;
            }
          }

          if (found_value) {
            send_bulk_string(client_fd, value);
          } else {
            send_null_bulk(client_fd);
          }
          continue;
        }
        send_pong(client_fd);
        continue;
      }

      const auto newline_pos = request_buffer.find('\n');
      if (newline_pos == std::string::npos) {
        break;
      }
      request_buffer.erase(0, newline_pos + 1);
      send_pong(client_fd);
    }
  }

  close(client_fd);
}

bool create_server_socket(int& server_fd) {
  server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) {
    std::cerr << "Failed to create server socket\n";
    return false;
  }

  // Since the tester restarts your program quite often, setting SO_REUSEADDR
  // ensures that we don't run into 'Address already in use' errors
  int reuse = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
    std::cerr << "setsockopt failed\n";
    return false;
  }

  struct sockaddr_in server_addr{};
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(kServerPort);

  if (bind(server_fd, reinterpret_cast<sockaddr*>(&server_addr), sizeof(server_addr)) != 0) {
    std::cerr << "Failed to bind to port " << kServerPort << "\n";
    return false;
  }

  if (listen(server_fd, kServerBacklog) != 0) {
    std::cerr << "listen failed\n";
    return false;
  }

  return true;
}

int main(int argc, char** argv) {
  // Flush after every std::cout / std::cerr
  std::cout << std::unitbuf;
  std::cerr << std::unitbuf;

  int server_fd = -1;
  if (!create_server_socket(server_fd)) {
   return 1;
  }

  std::cout << "Waiting for a client to connect...\n";

  // You can use print statements as follows for debugging, they'll be visible when running tests.
  std::cout << "Logs from your program will appear here!\n";

  while (true) {
    struct sockaddr_in client_addr{};
    socklen_t client_addr_len = sizeof(client_addr);
    const int client_fd = accept(server_fd, reinterpret_cast<sockaddr*>(&client_addr), &client_addr_len);
    if (client_fd < 0) {
      std::cerr << "Failed to accept client\n";
      continue;
    }
    std::cout << "Client connected\n";
    std::thread(handle_client, client_fd).detach();
  }
  
  close(server_fd);

  return 0;
}
