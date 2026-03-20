#ifndef REDIS_CPP_APPLICATION_HPP_
#define REDIS_CPP_APPLICATION_HPP_

#include "redis-cpp/config_parser.hpp"
#include "redis-cpp/result.hpp"

namespace redis {

class Application {
 public:
  int Run(int argc, char** argv);

 private:
  static int ExitWithError(const Error& error);

  ConfigParser config_parser_;
};

}  // namespace redis

#endif  // REDIS_CPP_APPLICATION_HPP_
