#include <iostream>

#include "redis-cpp/application.hpp"

int main(int argc, char** argv) {
  std::cout << std::unitbuf;
  std::cerr << std::unitbuf;

  redis::Application application;
  return application.Run(argc, argv);
}
