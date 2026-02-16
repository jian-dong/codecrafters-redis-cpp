#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <thread>
#include <climits>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>

constexpr int kServerPort = 6379;
constexpr int kServerBacklog = 5;
constexpr char kPongResponse[] = "+PONG\r\n";

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
bool is_complete_resp_array(const std::string& data, size_t& index) {
  int array_size = 0;
  if (!parse_number(data, index, array_size) || array_size < 0) {
    return false;
  }

  for (int arg = 0; arg < array_size; ++arg) {
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

    index += static_cast<size_t>(bulk_len);
    if (index + 1 >= data.size() || data[index] != '\r' || data[index + 1] != '\n') {
      return false;
    }

    index += 2;
  }

  return true;
}

void send_pong(int client_fd) {
  send(client_fd, kPongResponse, sizeof(kPongResponse) - 1, 0);
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
        i = 1;
        if (i >= request_buffer.size() || !is_complete_resp_array(request_buffer, i)) {
          break;
        }
        request_buffer.erase(0, i);
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

}
