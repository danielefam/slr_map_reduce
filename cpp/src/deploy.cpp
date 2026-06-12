#include "common.hpp"

#include <atomic>
#include <iostream>
#include <stdexcept>

namespace {

struct Manifest {
  std::string nfs;
  std::vector<std::string> files;
  int n = 0;
  std::vector<std::string> commands;
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
  auto nfs_values = jq_lines(path, ".nfs // \"\"");
  if (!nfs_values.empty()) {
    m.nfs = nfs_values.front();
  }
  m.files = jq_lines(path, ".files[]?");
  m.commands = jq_lines(path, ".commands[]?");
  return m;
}

void usage() {
  std::cerr << "Usage: slr_deploy -m manifest.json -h hosts.txt [-parallel N]\n";
}

std::string remote_script(const std::vector<std::string>& commands) {
  return slr::join(commands, " && ");
}

}  // namespace

int main(int argc, char** argv) {
  std::string manifest_path = "manifest.json";
  std::string hosts_path = "hosts.txt";
  int parallel = slr::kDefaultParallelism;

  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "-m" && i + 1 < argc) {
      manifest_path = argv[++i];
    } else if (arg == "-h" && i + 1 < argc) {
      hosts_path = argv[++i];
    } else if (arg == "-parallel" && i + 1 < argc) {
      parallel = std::stoi(argv[++i]);
    } else {
      usage();
      return 1;
    }
  }

  try {
    Manifest manifest = read_manifest(manifest_path);
    auto hosts = slr::read_hosts(hosts_path, manifest.n);

    if (!manifest.files.empty() && !manifest.nfs.empty()) {
      std::cout << "Copying " << manifest.files.size() << " file(s) to " << manifest.nfs << "...\n";
      for (const auto& file : manifest.files) {
        const std::string cmd = "scp " + slr::ssh_options() + " " +
                                slr::shell_quote(file) + " " + slr::shell_quote(manifest.nfs);
        const auto res = slr::exec_capture(cmd);
        if (res.exit_code != 0) {
          std::cerr << "error copying " << file << ":\n" << res.output;
          return 1;
        }
      }
    }

    std::cout << "Deploying to " << hosts.size() << " host(s)...\n";
    std::atomic<int> failed{0};
    slr::parallel_for(hosts.size(), parallel, [&](size_t idx) {
      const std::string& host = hosts[idx];
      const std::string cmd = "ssh " + slr::ssh_options() + " " + slr::shell_quote(host) + " " +
                              slr::shell_quote(remote_script(manifest.commands));
      const auto res = slr::exec_capture(cmd);
      if (res.exit_code != 0) {
        std::cerr << "[" << host << "] ERROR:\n" << res.output;
        failed.fetch_add(1);
      } else {
        std::cout << "[" << host << "] OK\n" << res.output;
      }
    });

    if (failed.load() > 0) {
      std::cerr << failed.load() << " host(s) failed\n";
      return 1;
    }

    std::cout << "Done.\n";
  } catch (const std::exception& ex) {
    std::cerr << "error: " << ex.what() << "\n";
    return 1;
  }

  return 0;
}
