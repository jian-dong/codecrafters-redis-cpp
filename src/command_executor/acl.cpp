#include "redis-cpp/command_executor.hpp"

#include <array>
#include <cstdint>
#include <iomanip>
#include <mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

namespace redis {
namespace {

uint32_t RotateRight(uint32_t value, int shift) {
  return (value >> shift) | (value << (32 - shift));
}

std::string Sha256Hex(std::string_view input) {
  static constexpr std::array<uint32_t, 64> kRoundConstants = {
      0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b,
      0x59f111f1, 0x923f82a4, 0xab1c5ed5, 0xd807aa98, 0x12835b01,
      0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7,
      0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc,
      0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152,
      0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147,
      0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
      0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
      0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819,
      0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116, 0x1e376c08,
      0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f,
      0x682e6ff3, 0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
      0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
  };
  static constexpr std::array<uint32_t, 8> kInitialState = {
      0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
      0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
  };

  std::vector<uint8_t> message(input.begin(), input.end());
  const uint64_t bit_length = static_cast<uint64_t>(message.size()) * 8ULL;
  message.push_back(0x80);
  while ((message.size() % 64) != 56) {
    message.push_back(0);
  }
  for (int shift = 56; shift >= 0; shift -= 8) {
    message.push_back(static_cast<uint8_t>((bit_length >> shift) & 0xffU));
  }

  std::array<uint32_t, 8> state = kInitialState;
  std::array<uint32_t, 64> schedule = {};
  for (size_t offset = 0; offset < message.size(); offset += 64) {
    for (size_t index = 0; index < 16; ++index) {
      const size_t base = offset + (index * 4);
      schedule[index] = (static_cast<uint32_t>(message[base]) << 24) |
                        (static_cast<uint32_t>(message[base + 1]) << 16) |
                        (static_cast<uint32_t>(message[base + 2]) << 8) |
                        static_cast<uint32_t>(message[base + 3]);
    }
    for (size_t index = 16; index < schedule.size(); ++index) {
      const uint32_t s0 = RotateRight(schedule[index - 15], 7) ^
                          RotateRight(schedule[index - 15], 18) ^
                          (schedule[index - 15] >> 3);
      const uint32_t s1 = RotateRight(schedule[index - 2], 17) ^
                          RotateRight(schedule[index - 2], 19) ^
                          (schedule[index - 2] >> 10);
      schedule[index] =
          schedule[index - 16] + s0 + schedule[index - 7] + s1;
    }

    uint32_t a = state[0];
    uint32_t b = state[1];
    uint32_t c = state[2];
    uint32_t d = state[3];
    uint32_t e = state[4];
    uint32_t f = state[5];
    uint32_t g = state[6];
    uint32_t h = state[7];

    for (size_t index = 0; index < schedule.size(); ++index) {
      const uint32_t sum1 =
          RotateRight(e, 6) ^ RotateRight(e, 11) ^ RotateRight(e, 25);
      const uint32_t choice = (e & f) ^ ((~e) & g);
      const uint32_t temp1 =
          h + sum1 + choice + kRoundConstants[index] + schedule[index];
      const uint32_t sum0 =
          RotateRight(a, 2) ^ RotateRight(a, 13) ^ RotateRight(a, 22);
      const uint32_t majority = (a & b) ^ (a & c) ^ (b & c);
      const uint32_t temp2 = sum0 + majority;

      h = g;
      g = f;
      f = e;
      e = d + temp1;
      d = c;
      c = b;
      b = a;
      a = temp1 + temp2;
    }

    state[0] += a;
    state[1] += b;
    state[2] += c;
    state[3] += d;
    state[4] += e;
    state[5] += f;
    state[6] += g;
    state[7] += h;
  }

  std::ostringstream output;
  output << std::hex << std::setfill('0');
  for (uint32_t value : state) {
    output << std::setw(8) << value;
  }
  return output.str();
}

std::string EncodeAclGetuserResponse(bool nopass,
                                     const std::vector<std::string>& passwords) {
  std::string response = "*4\r\n";
  response += RespWriter::Write(RespBulkString{"flags"});
  if (nopass) {
    response += "*1\r\n";
    response += RespWriter::Write(RespBulkString{"nopass"});
  } else {
    response += "*0\r\n";
  }
  response += RespWriter::Write(RespBulkString{"passwords"});
  response += "*" + std::to_string(passwords.size()) + "\r\n";
  for (const std::string& password : passwords) {
    response += RespWriter::Write(RespBulkString{password});
  }
  return response;
}

}  // namespace

bool CommandExecutor::DefaultUserStartsAuthenticated() const {
  return DefaultUserUsesNoPassword();
}

bool CommandExecutor::DefaultUserAcceptsPassword(
    const std::string& password) const {
  const std::string password_hash = Sha256Hex(password);

  std::lock_guard<std::mutex> lock(acl_mutex_);
  if (default_user_.nopass) {
    return true;
  }

  for (const std::string& stored_hash : default_user_.password_hashes) {
    if (stored_hash == password_hash) {
      return true;
    }
  }

  return false;
}

RespRaw CommandExecutor::DefaultUserDescription() const {
  std::lock_guard<std::mutex> lock(acl_mutex_);
  return RespRaw{EncodeAclGetuserResponse(default_user_.nopass,
                                          default_user_.password_hashes)};
}

void CommandExecutor::SetDefaultUserPassword(const std::string& password) {
  std::lock_guard<std::mutex> lock(acl_mutex_);
  default_user_.nopass = false;
  default_user_.password_hashes = {Sha256Hex(password)};
}

bool CommandExecutor::DefaultUserUsesNoPassword() const {
  std::lock_guard<std::mutex> lock(acl_mutex_);
  return default_user_.nopass;
}

CommandResult CommandExecutor::HandleAuth(const std::vector<std::string>& args) {
  if (args.size() != 3) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongArity, .command = "auth"});
  }

  if (args[1] != "default") {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kWrongPass, .command = "auth"});
  }

  if (DefaultUserAcceptsPassword(args[2])) {
    return RespSimpleString{"OK"};
  }

  return tl::make_unexpected(
      CommandError{.code = CommandErrorCode::kWrongPass, .command = "auth"});
}

CommandResult CommandExecutor::HandleAcl(const std::vector<std::string>& args) {
  if (args.size() == 2 && ToUpperAscii(args[1]) == "WHOAMI") {
    return RespBulkString{"default"};
  }

  if (args.size() == 3 && ToUpperAscii(args[1]) == "GETUSER") {
    if (args[2] != "default") {
      return RespNullBulk{};
    }

    return DefaultUserDescription();
  }

  if (args.size() == 4 && ToUpperAscii(args[1]) == "SETUSER") {
    if (args[2] != "default" || !args[3].starts_with(">")) {
      return tl::make_unexpected(
          CommandError{.code = CommandErrorCode::kSyntaxError, .command = "acl"});
    }

    SetDefaultUserPassword(args[3].substr(1));
    return RespSimpleString{"OK"};
  }

  if (args.size() == 2 || args.size() == 3) {
    return tl::make_unexpected(
        CommandError{.code = CommandErrorCode::kSyntaxError, .command = "acl"});
  }

  return tl::make_unexpected(
      CommandError{.code = CommandErrorCode::kWrongArity, .command = "acl"});
}

}  // namespace redis
