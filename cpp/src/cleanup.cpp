#include "common.hpp"

#include <atomic>
#include <filesystem>
#include <iostream>
#include <stdexcept>

namespace {

struct Manifest {
  std::string nfs;
  std::vector<std::string> files;
  int n = 0;
  std::vector<std::string> cleanup_commands;
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
  m.cleanup_commands = jq_lines(path, ".cleanup_commands[]?");
  return m;
}

void usage() {
  std::cerr << "Usage: slr_cleanup -m manifest.json -h hosts.txt [-parallel N]\n";
}

std::string remove_nfs_script(const std::string& nfs, const std::vector<std::string>& files) {
  const auto pos = nfs.find(':');
  if (pos == std::string::npos) {
    throw std::runtime_error("invalid nfs target (expected [user@]host:/path): " + nfs);
  }
  const std::string base = nfs.substr(pos + 1);

  std::vector<std::string> remote_paths;
  remote_paths.reserve(files.size());
  for (const auto& file : files) {
    remote_paths.push_back(base + "/" + std::filesystem::path(file).filename().string());
  }

  std::vector<std::string> quoted;
  quoted.reserve(remote_paths.size());
  for (const auto& p : remote_paths) {
    quoted.push_back(slr::shell_quote(p));
  }
  return "rm -f -- " + slr::join(quoted, " ");
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
    const Manifest manifest = read_manifest(manifest_path);
    const auto hosts = slr::read_hosts(hosts_path, manifest.n);
    int exit_code = 0;

    if (!manifest.cleanup_commands.empty()) {
      std::cout << "Running cleanup commands on " << hosts.size() << " host(s)...\n";
      std::atomic<int> failed{0};
      slr::parallel_for(hosts.size(), parallel, [&](size_t idx) {
        const std::string cmd = "ssh " + slr::ssh_options() + " " + slr::shell_quote(hosts[idx]) + " " +
                                slr::shell_quote(slr::join(manifest.cleanup_commands, " && "));
        const auto res = slr::exec_capture(cmd);
        if (res.exit_code != 0) {
          std::cerr << "[" << hosts[idx] << "] ERROR:\n" << res.output;
          failed.fetch_add(1);
        } else {
          std::cout << "[" << hosts[idx] << "] OK\n" << res.output;
        }
      });
      if (failed.load() > 0) {
        exit_code = 1;
      }
    }

    if (!manifest.files.empty() && !manifest.nfs.empty()) {
      std::cout << "Removing files from NFS (" << manifest.nfs << ")...\n";
      const auto pos = manifest.nfs.find(':');
      if (pos == std::string::npos) {
        throw std::runtime_error("invalid nfs target (expected [user@]host:/path): " + manifest.nfs);
      }
      const std::string ssh_host = manifest.nfs.substr(0, pos);
      const std::string cmd = "ssh " + slr::ssh_options() + " " + slr::shell_quote(ssh_host) + " " +
                              slr::shell_quote(remove_nfs_script(manifest.nfs, manifest.files));
      const auto res = slr::exec_capture(cmd);
      if (res.exit_code != 0) {
        std::cerr << "error removing files from NFS:\n" << res.output;
        exit_code = 1;
      }
    }

    if (exit_code == 0) {
      std::cout << "Cleanup done.\n";
    }
    return exit_code;
  } catch (const std::exception& ex) {
    std::cerr << "error: " << ex.what() << "\n";
    return 1;
  }
}
