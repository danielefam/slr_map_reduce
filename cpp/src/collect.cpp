#include "common.hpp"

#include <atomic>
#include <iostream>
#include <mutex>
#include <stdexcept>

namespace {

struct Manifest {
  int n = 0;
  std::vector<std::string> collect_commands;
};

std::vector<std::string> jq_lines(const std::string& manifest, const std::string& expr) {
  const std::string cmd = "jq -r " + slr::shell_quote(expr) + " " + slr::shell_quote(manifest);
  const auto res = slr::exec_capture(cmd);
  if (res.exit_code != 0) {
    throw std::runtime_error("jq failed: " + res.output);
  }
  std::vector<std::string> out;
  for (const auto& line : slr::split_lines(res.output)) {
    if (!line.empty() && line != "null") {
      out.push_back(line);
    }
  }
  return out;
}

Manifest read_manifest(const std::string& path) {
  Manifest m;
  auto n_values = jq_lines(path, ".n // 0");
  if (!n_values.empty()) {
    m.n = std::stoi(n_values.front());
  }
  m.collect_commands = jq_lines(path, ".collect_commands[]?");
  return m;
}

void usage() {
  std::cerr << "Usage: slr_collect -m manifest.json -h hosts.txt -o stats.txt [-parallel N]\n";
}

struct HostResult {
  std::string host;
  std::string output;
  bool ok;
};

}  // namespace

int main(int argc, char** argv) {
  std::string manifest_path = "manifest.json";
  std::string hosts_path = "hosts.txt";
  std::string output_path = "stats.txt";
  int parallel = slr::kDefaultParallelism;

  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "-m" && i + 1 < argc) {
      manifest_path = argv[++i];
    } else if (arg == "-h" && i + 1 < argc) {
      hosts_path = argv[++i];
    } else if (arg == "-o" && i + 1 < argc) {
      output_path = argv[++i];
    } else if (arg == "-parallel" && i + 1 < argc) {
      parallel = std::stoi(argv[++i]);
    } else {
      usage();
      return 1;
    }
  }

  try {
    const Manifest manifest = read_manifest(manifest_path);
    if (manifest.collect_commands.empty()) {
      std::cerr << "no collect_commands defined in manifest\n";
      return 1;
    }

    const auto hosts = slr::read_hosts(hosts_path, manifest.n);
    std::cout << "Collecting stats from " << hosts.size() << " host(s)...\n";

    std::vector<HostResult> results(hosts.size());
    std::atomic<int> failed{0};

    slr::parallel_for(hosts.size(), parallel, [&](size_t idx) {
      const std::string script = slr::join(manifest.collect_commands, " && ");
      const std::string cmd = "ssh " + slr::ssh_options() + " " + slr::shell_quote(hosts[idx]) + " " +
                              slr::shell_quote(script);
      const auto res = slr::exec_capture(cmd);
      results[idx] = HostResult{hosts[idx], res.output, res.exit_code == 0};
      if (res.exit_code != 0) {
        failed.fetch_add(1);
      }
    });

    std::string out;
    for (const auto& r : results) {
      out += "=== " + r.host + " ===\n";
      if (!r.ok) {
        out += "ERROR\n";
      }
      out += r.output;
      if (!out.empty() && out.back() != '\n') {
        out += "\n";
      }
      out += "\n";
    }

    slr::write_file(output_path, out);
    std::cout << "Saved stats from " << (hosts.size() - static_cast<size_t>(failed.load()))
              << " host(s) to " << output_path << "\n";

    if (failed.load() > 0) {
      std::cerr << failed.load() << " host(s) failed\n";
      return 1;
    }
  } catch (const std::exception& ex) {
    std::cerr << "error: " << ex.what() << "\n";
    return 1;
  }

  return 0;
}
