#ifndef REDIS_CPP_UNIQUE_FD_HPP_
#define REDIS_CPP_UNIQUE_FD_HPP_

#include <unistd.h>

namespace redis {

class UniqueFd {
 public:
  UniqueFd() = default;
  explicit UniqueFd(int fd) : fd_(fd) {}

  UniqueFd(const UniqueFd&) = delete;
  UniqueFd& operator=(const UniqueFd&) = delete;

  UniqueFd(UniqueFd&& other) noexcept : fd_(other.Release()) {}

  UniqueFd& operator=(UniqueFd&& other) noexcept {
    if (this != &other) {
      Reset(other.Release());
    }
    return *this;
  }

  ~UniqueFd() { Reset(); }

  [[nodiscard]] int Get() const { return fd_; }
  [[nodiscard]] bool IsValid() const { return fd_ >= 0; }

  int Release() {
    const int released_fd = fd_;
    fd_ = -1;
    return released_fd;
  }

  void Reset(int fd = -1) {
    if (fd_ >= 0) {
      close(fd_);
    }
    fd_ = fd;
  }

 private:
  int fd_ = -1;
};

}  // namespace redis

#endif  // REDIS_CPP_UNIQUE_FD_HPP_
