#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <thread>
#include <climits>
#include <vector>
#include <unordered_map>
#include <mutex>
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

std::mutex gStoreMutex;
std::unordered_map<std::string, std::string> gStore;

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
        if (!args.empty() && args[0] == "PING") {
          send_pong(client_fd);
          continue;
        }
        if (!args.empty() && args[0] == "ECHO" && args.size() >= 2) {
          send_bulk_string(client_fd, args[1]);
          continue;
        }
        if (!args.empty() && args[0] == "SET" && args.size() >= 3) {
          {
            std::lock_guard<std::mutex> lock(gStoreMutex);
            gStore[args[1]] = args[2];
          }
          send_ok(client_fd);
          continue;
        }
        if (!args.empty() && args[0] == "GET" && args.size() >= 2) {
          std::string value;
          {
            std::lock_guard<std::mutex> lock(gStoreMutex);
            const auto found = gStore.find(args[1]);
            if (found == gStore.end()) {
              send_null_bulk(client_fd);
              continue;
            }
            value = found->second;
          }
          send_bulk_string(client_fd, value);
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
