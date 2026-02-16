#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <thread>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>

void handle_client(int client_fd) {
  std::string request_buffer;
  const char *response = "+PONG\r\n";

  auto parse_number = [](const std::string &data, size_t &index, int &value) -> bool {
    size_t start = index;
    while (index < data.size() && data[index] >= '0' && data[index] <= '9') {
      index++;
    }

    if (start == index || index + 1 >= data.size() || data[index] != '\r' || data[index + 1] != '\n') {
      return false;
    }

    value = std::stoi(data.substr(start, index - start));
    index += 2;
    return true;
  };

  while (true) {
    char buffer[1024];
    ssize_t bytes_received = recv(client_fd, buffer, sizeof(buffer), 0);
    if (bytes_received <= 0) {
      break;
    }

    request_buffer.append(buffer, bytes_received);

    while (!request_buffer.empty()) {
      size_t i = 0;
      if (request_buffer[0] == '*') {
        i = 1;
        int array_size = 0;
        if (i >= request_buffer.size() || !parse_number(request_buffer, i, array_size)) {
          break;
        }

        bool complete = true;
        for (int arg = 0; arg < array_size; ++arg) {
          if (i >= request_buffer.size() || request_buffer[i] != '$') {
            complete = false;
            break;
          }
          i++;
          int bulk_len = 0;
          if (!parse_number(request_buffer, i, bulk_len)) {
            complete = false;
            break;
          }
          if (i + static_cast<size_t>(bulk_len) + 2 > request_buffer.size()) {
            complete = false;
            break;
          }
          i += bulk_len;
          if (i + 1 >= request_buffer.size() || request_buffer[i] != '\r' || request_buffer[i + 1] != '\n') {
            complete = false;
            break;
          }
          i += 2;
        }

        if (!complete) {
          break;
        }

        request_buffer.erase(0, i);
        send(client_fd, response, strlen(response), 0);
        continue;
      }

      auto newline_pos = request_buffer.find('\n');
      if (newline_pos == std::string::npos) {
        break;
      }
      request_buffer.erase(0, newline_pos + 1);
      send(client_fd, response, strlen(response), 0);
    }
  }

  close(client_fd);
}

int main(int argc, char **argv) {
  // Flush after every std::cout / std::cerr
  std::cout << std::unitbuf;
  std::cerr << std::unitbuf;
  
  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) {
   std::cerr << "Failed to create server socket\n";
   return 1;
  }
  
  // Since the tester restarts your program quite often, setting SO_REUSEADDR
  // ensures that we don't run into 'Address already in use' errors
  int reuse = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
    std::cerr << "setsockopt failed\n";
    return 1;
  }
  
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(6379);
  
  if (bind(server_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) != 0) {
    std::cerr << "Failed to bind to port 6379\n";
    return 1;
  }
  
  int connection_backlog = 5;
  if (listen(server_fd, connection_backlog) != 0) {
    std::cerr << "listen failed\n";
    return 1;
  }
  
  struct sockaddr_in client_addr;
  std::cout << "Waiting for a client to connect...\n";

  // You can use print statements as follows for debugging, they'll be visible when running tests.
  std::cout << "Logs from your program will appear here!\n";

  while (true) {
    socklen_t client_addr_len = sizeof(client_addr);
    int client_fd = accept(server_fd, (struct sockaddr *) &client_addr, &client_addr_len);
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
