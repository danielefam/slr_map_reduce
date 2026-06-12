#include "common.hpp"

#include <array>
#include <atomic>
#include <exception>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <thread>

#include <sys/wait.h>

namespace slr {

std::string trim(const std::string& s) {
  const auto first = s.find_first_not_of(" \t\r\n");
  if (first == std::string::npos) {
    return "";
  }
  const auto last = s.find_last_not_of(" \t\r\n");
  return s.substr(first, last - first + 1);
}

std::vector<std::string> split_lines(const std::string& s) {
  std::vector<std::string> out;
  std::stringstream ss(s);
  std::string line;
  while (std::getline(ss, line)) {
    out.push_back(trim(line));
  }
  return out;
}

std::string join(const std::vector<std::string>& values, const std::string& sep) {
  std::ostringstream out;
  for (size_t i = 0; i < values.size(); ++i) {
    if (i > 0) {
      out << sep;
    }
    out << values[i];
  }
  return out.str();
}

std::string read_file(const std::string& path) {
  std::ifstream in(path);
  if (!in) {
    throw std::runtime_error("failed to open file: " + path);
  }
  std::ostringstream ss;
  ss << in.rdbuf();
  return ss.str();
}

void write_file(const std::string& path, const std::string& content) {
  std::ofstream out(path);
  if (!out) {
    throw std::runtime_error("failed to write file: " + path);
  }
  out << content;
}

std::string shell_quote(const std::string& s) {
  std::string out = "'";
  for (char c : s) {
    if (c == '\'') {
      out += "'\\''";
    } else {
      out.push_back(c);
    }
  }
  out += "'";
  return out;
}

CommandResult exec_capture(const std::string& command) {
  std::array<char, 4096> buffer{};
  std::string output;

  FILE* pipe = popen((command + " 2>&1").c_str(), "r");
  if (pipe == nullptr) {
    throw std::runtime_error("failed to start command: " + command);
  }

  while (fgets(buffer.data(), static_cast<int>(buffer.size()), pipe) != nullptr) {
    output += buffer.data();
  }

  const int rc = pclose(pipe);
  int exit_code = rc;
  if (WIFEXITED(rc)) {
    exit_code = WEXITSTATUS(rc);
  }
  return CommandResult{exit_code, output};
}

int exec_no_capture(const std::string& command) {
  const int rc = std::system(command.c_str());
  if (WIFEXITED(rc)) {
    return WEXITSTATUS(rc);
  }
  return rc;
}

std::vector<std::string> read_hosts(const std::string& path, int n_limit) {
  std::vector<std::string> hosts;
  for (const std::string& line : split_lines(read_file(path))) {
    if (!line.empty()) {
      hosts.push_back(line);
    }
  }
  if (n_limit > 0 && static_cast<int>(hosts.size()) > n_limit) {
    hosts.resize(static_cast<size_t>(n_limit));
  }
  return hosts;
}

void parallel_for(size_t total, int parallelism, const std::function<void(size_t)>& fn) {
  if (total == 0) {
    return;
  }
  if (parallelism <= 0) {
    parallelism = kDefaultParallelism;
  }
  if (parallelism > static_cast<int>(total)) {
    parallelism = static_cast<int>(total);
  }

  std::atomic<size_t> next{0};
  std::exception_ptr first_error;
  std::mutex error_mu;
  std::vector<std::thread> threads;
  threads.reserve(static_cast<size_t>(parallelism));
  for (int i = 0; i < parallelism; ++i) {
    threads.emplace_back([&]() {
      while (true) {
        {
          std::lock_guard<std::mutex> lock(error_mu);
          if (first_error) {
            return;
          }
        }
        const size_t idx = next.fetch_add(1);
        if (idx >= total) {
          break;
        }
        try {
          fn(idx);
        } catch (...) {
          std::lock_guard<std::mutex> lock(error_mu);
          if (!first_error) {
            first_error = std::current_exception();
          }
          return;
        }
      }
    });
  }
  for (auto& t : threads) {
    t.join();
  }
  if (first_error) {
    std::rethrow_exception(first_error);
  }
}

std::string ssh_options() {
  const std::string control_path = std::filesystem::temp_directory_path().string() + "/slr-ssh-%C";
  return "-o StrictHostKeyChecking=no "
         "-o BatchMode=yes "
         "-o ConnectTimeout=10 "
         "-o ServerAliveInterval=5 "
         "-o ServerAliveCountMax=3 "
         "-o ControlMaster=auto "
         "-o ControlPersist=60s "
         "-o ControlPath=" + shell_quote(control_path);
}

}  // namespace slr
