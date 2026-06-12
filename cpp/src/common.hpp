#pragma once

#include <cstddef>
#include <functional>
#include <string>
#include <vector>

namespace slr {

struct CommandResult {
  int exit_code;
  std::string output;
};

std::string trim(const std::string& s);
std::vector<std::string> split_lines(const std::string& s);
std::string join(const std::vector<std::string>& values, const std::string& sep);

std::string read_file(const std::string& path);
void write_file(const std::string& path, const std::string& content);

std::string shell_quote(const std::string& s);
CommandResult exec_capture(const std::string& command);
int exec_no_capture(const std::string& command);

std::vector<std::string> read_hosts(const std::string& path, int n_limit);

void parallel_for(size_t total, int parallelism, const std::function<void(size_t)>& fn);

constexpr int kDefaultParallelism = 16;

std::string ssh_options();

}  // namespace slr
