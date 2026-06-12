#include "common.hpp"

#include <cstdlib>
#include <iostream>
#include <stdexcept>

namespace {

void usage() {
  std::cerr << "Usage: slr_make_hosts [-n N] [-f FILE]\n";
}

}  // namespace

int main(int argc, char** argv) {
  int n = 10;
  std::string out_file = "hosts.txt";

  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "-n" && i + 1 < argc) {
      n = std::stoi(argv[++i]);
    } else if (arg == "-f" && i + 1 < argc) {
      out_file = argv[++i];
    } else {
      usage();
      return 1;
    }
  }

  try {
    // Parse available hosts using jq to keep this tool dependency-light.
    const std::string cmd =
        "curl -fsSL https://tp.telecom-paris.fr/ajax.php | "
        "jq -r '.data[] | select(.[1] == true) | .[0] + \".enst.fr\"'";
    const auto result = slr::exec_capture(cmd);
    if (result.exit_code != 0) {
      std::cerr << "error fetching hosts:\n" << result.output;
      return 1;
    }

    std::vector<std::string> hosts;
    for (const std::string& line : slr::split_lines(result.output)) {
      if (!line.empty()) {
        hosts.push_back(line);
      }
    }

    if (n > 0 && static_cast<int>(hosts.size()) > n) {
      hosts.resize(static_cast<size_t>(n));
    }

    std::string content;
    for (const auto& host : hosts) {
      content += host + "\n";
    }
    slr::write_file(out_file, content);
    std::cout << "Wrote " << hosts.size() << " hosts to " << out_file << "\n";
  } catch (const std::exception& ex) {
    std::cerr << "error: " << ex.what() << "\n";
    return 1;
  }

  return 0;
}
