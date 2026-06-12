#include "common.hpp"
#include "job.hpp"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <cstdlib>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <numeric>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

namespace {

constexpr size_t kChunkSize = 64u * 1024u * 1024u;

std::string user_tag() {
  const char* user_env = std::getenv("USER");
  std::string user = (user_env != nullptr && *user_env != '\0') ? std::string(user_env) : "unknown";
  for (char& ch : user) {
    const unsigned char uch = static_cast<unsigned char>(ch);
    if (!(std::isalnum(uch) || ch == '-' || ch == '_')) {
      ch = '_';
    }
  }
  return user;
}

std::string worker_binary_path() {
  static const std::string path = "/tmp/mr-worker-" + user_tag();
  return path;
}

std::string worker_pid_path() {
  return worker_binary_path() + ".pid";
}

std::string worker_log_path() {
  return worker_binary_path() + ".log";
}

struct Config {
  std::string hosts_file = "../hosts.txt";
  std::string input_file;
  bool commoncrawl = false;
  std::string crawl;
  int files_limit = 0;
  int chunks_limit = 0;
  std::string output_file = "result.txt";
  std::string job = "wordcount";
  int n = 0;
  int parallel = slr::kDefaultParallelism;
  int port = 9090;
  bool worker_pprof = false;
};

std::string detect_job_name(const std::string& job_arg) {
  fs::path p(job_arg);
  std::string name = p.filename().string();
  if (name.empty()) {
    name = job_arg;
  }
  std::transform(name.begin(), name.end(), name.begin(), [](unsigned char c) {
    return static_cast<char>(std::tolower(c));
  });
  if (name.empty()) {
    throw std::runtime_error("empty job argument");
  }
  return name;
}

Config parse_args(int argc, char** argv) {
  Config cfg;
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "-hosts" && i + 1 < argc) {
      cfg.hosts_file = argv[++i];
    } else if (arg == "-input" && i + 1 < argc) {
      cfg.input_file = argv[++i];
    } else if (arg == "-commoncrawl") {
      cfg.commoncrawl = true;
    } else if (arg == "-crawl" && i + 1 < argc) {
      cfg.crawl = argv[++i];
    } else if (arg == "-files-limit" && i + 1 < argc) {
      cfg.files_limit = std::stoi(argv[++i]);
    } else if (arg == "-chunks-limit" && i + 1 < argc) {
      cfg.chunks_limit = std::stoi(argv[++i]);
    } else if (arg == "-output" && i + 1 < argc) {
      cfg.output_file = argv[++i];
    } else if (arg == "-job" && i + 1 < argc) {
      cfg.job = argv[++i];
    } else if (arg == "-n" && i + 1 < argc) {
      cfg.n = std::stoi(argv[++i]);
    } else if (arg == "-parallel" && i + 1 < argc) {
      cfg.parallel = std::stoi(argv[++i]);
    } else if (arg == "-port" && i + 1 < argc) {
      cfg.port = std::stoi(argv[++i]);
    } else if (arg == "-worker-pprof") {
      cfg.worker_pprof = true;
    } else {
      throw std::runtime_error("unknown or incomplete flag: " + arg);
    }
  }

  if (cfg.job.empty()) {
    throw std::runtime_error("missing -job");
  }
  if ((cfg.input_file.empty() && !cfg.commoncrawl) || (!cfg.input_file.empty() && cfg.commoncrawl)) {
    throw std::runtime_error("choose exactly one of -input <file> or -commoncrawl");
  }
  return cfg;
}

std::string scripts_root() {
  fs::path cwd = fs::current_path();
  if (fs::exists(cwd / "scripts")) {
    return cwd.string();
  }
  if (fs::exists(cwd / "../scripts")) {
    return fs::weakly_canonical(cwd / "..").string();
  }
  return cwd.string();
}

std::vector<std::string> health_ready(const std::vector<std::string>& peers, int retries, std::chrono::milliseconds delay, int parallel) {
  std::vector<bool> ok(peers.size(), false);
  for (int attempt = 0; attempt < retries; ++attempt) {
    slr::parallel_for(peers.size(), parallel, [&](size_t i) {
      if (ok[i]) {
        return;
      }
      const std::string cmd = "curl -fsS --max-time 5 " + slr::shell_quote("http://" + peers[i] + "/health") + " >/dev/null";
      ok[i] = slr::exec_no_capture(cmd) == 0;
    });
    if (std::all_of(ok.begin(), ok.end(), [](bool v) { return v; })) {
      break;
    }
    std::this_thread::sleep_for(delay);
  }

  std::vector<std::string> out;
  for (size_t i = 0; i < peers.size(); ++i) {
    if (ok[i]) {
      out.push_back(peers[i]);
    }
  }
  return out;
}

void deploy_workers(const std::vector<std::string>& hosts, const std::string& worker_local_path, int port, const std::string& job_name, bool pprof, int parallel) {
  const std::string worker_binary = worker_binary_path();
  const std::string worker_pid = worker_pid_path();
  const std::string worker_log = worker_log_path();
  slr::parallel_for(hosts.size(), parallel, [&](size_t i) {
    const std::string& host = hosts[i];
    const std::string remote_binary = worker_binary + ".deploy-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + "-" + std::to_string(i);
    const std::string scp_cmd = "scp " + slr::ssh_options() + " " + slr::shell_quote(worker_local_path) + " " + slr::shell_quote(host + ":" + remote_binary);
    auto scp = slr::exec_capture(scp_cmd);
    if (scp.exit_code != 0) {
      throw std::runtime_error("scp failed on " + host + ": " + scp.output);
    }

    const std::string start_cmd =
      "kill $(cat " + worker_pid + ") 2>/dev/null || true && "
      "rm -f " + worker_pid + " " + worker_log + " " + worker_binary + " && "
      "mv " + remote_binary + " " + worker_binary + " && "
      "chmod +x " + worker_binary + " && "
      "nohup " + worker_binary + " -port " + std::to_string(port) + " -job " + job_name + (pprof ? " -pprof" : "") +
      " </dev/null >" + worker_log + " 2>&1 & echo $! > " + worker_pid;

    const std::string ssh_cmd = "ssh " + slr::ssh_options() + " " + slr::shell_quote(host) + " " + slr::shell_quote(start_cmd);
    auto ssh = slr::exec_capture(ssh_cmd);
    if (ssh.exit_code != 0) {
      throw std::runtime_error("ssh start failed on " + host + ": " + ssh.output);
    }
  });
}

std::vector<std::string> split_chunks_round_robin(const std::string& input, int n) {
  std::vector<std::string> chunks;
  chunks.resize(static_cast<size_t>(n));
  std::string data = slr::read_file(input);

  std::vector<std::string> pieces;
  size_t offset = 0;
  while (offset < data.size()) {
    size_t end = std::min(offset + kChunkSize, data.size());
    if (end < data.size()) {
      size_t newline = data.rfind('\n', end);
      if (newline != std::string::npos && newline >= offset) {
        end = newline + 1;
      }
    }
    if (end <= offset) {
      end = std::min(offset + kChunkSize, data.size());
    }
    pieces.push_back(data.substr(offset, end - offset));
    offset = end;
  }

  for (size_t i = 0; i < pieces.size(); ++i) {
    chunks[i % static_cast<size_t>(n)] += pieces[i];
    if (!chunks[i % static_cast<size_t>(n)].empty() && chunks[i % static_cast<size_t>(n)].back() != '\n') {
      chunks[i % static_cast<size_t>(n)].push_back('\n');
    }
  }
  return chunks;
}

std::vector<std::string> resolve_commoncrawl_urls(std::string crawl, int files_limit, int chunks_limit) {
  if (crawl.empty()) {
    auto res = slr::exec_capture("curl -fsSL https://index.commoncrawl.org/collinfo.json | jq -r '.[].id' | sort | tail -n1");
    if (res.exit_code != 0) {
      throw std::runtime_error("resolve latest crawl: " + res.output);
    }
    crawl = slr::trim(res.output);
    if (crawl.empty()) {
      throw std::runtime_error("latest crawl id is empty");
    }
  }

  const std::string cmd = "curl -fsSL " + slr::shell_quote("https://data.commoncrawl.org/crawl-data/" + crawl + "/wet.paths.gz") + " | gzip -dc";
  auto res = slr::exec_capture(cmd);
  if (res.exit_code != 0) {
    throw std::runtime_error("fetch wet paths: " + res.output);
  }

  std::vector<std::string> urls;
  for (const auto& line : slr::split_lines(res.output)) {
    if (line.empty()) {
      continue;
    }
    urls.push_back("https://data.commoncrawl.org/" + line);
  }
  std::sort(urls.begin(), urls.end());

  int limit = static_cast<int>(urls.size());
  if (files_limit > 0 && files_limit < limit) {
    limit = files_limit;
  }
  if (chunks_limit > 0 && chunks_limit < limit) {
    limit = chunks_limit;
  }
  urls.resize(static_cast<size_t>(limit));
  if (urls.empty()) {
    throw std::runtime_error("no common crawl URLs selected");
  }
  return urls;
}

void post_payload(const std::string& peer, const std::string& route, const std::string& body) {
  fs::path tmp = fs::temp_directory_path() / ("slr-http-" + std::to_string(::getpid()) + "-" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
  slr::write_file(tmp.string(), body);
  const std::string cmd =
      "curl -fsS --max-time 1800 -X POST --data-binary @" + slr::shell_quote(tmp.string()) + " " +
      slr::shell_quote("http://" + peer + route) + " >/dev/null";
  auto res = slr::exec_capture(cmd);
  std::error_code ec;
  fs::remove(tmp, ec);
  if (res.exit_code != 0) {
    throw std::runtime_error("POST " + peer + route + " failed: " + res.output);
  }
}

std::string get_payload(const std::string& peer, const std::string& route) {
  const std::string cmd = "curl -fsS --max-time 1800 " + slr::shell_quote("http://" + peer + route);
  auto res = slr::exec_capture(cmd);
  if (res.exit_code != 0) {
    throw std::runtime_error("GET " + peer + route + " failed: " + res.output);
  }
  return res.output;
}

void cleanup_hosts(const std::vector<std::string>& hosts, int parallel) {
  const std::string worker_binary = worker_binary_path();
  const std::string worker_pid = worker_pid_path();
  const std::string worker_log = worker_log_path();
  slr::parallel_for(hosts.size(), parallel, [&](size_t i) {
    const std::string cmd =
        "ssh " + slr::ssh_options() + " " + slr::shell_quote(hosts[i]) + " " +
        slr::shell_quote("kill $(cat " + worker_pid + ") 2>/dev/null || true && rm -f " + worker_pid + " " + worker_log + " " + worker_binary + " && rm -rf /tmp/slr-worker-*");
    (void)slr::exec_capture(cmd);
  });
}

std::vector<slr::KeyValue> parse_result_lines(const std::string& text) {
  std::vector<slr::KeyValue> kvs;
  std::stringstream ss(text);
  std::string line;
  while (std::getline(ss, line)) {
    const auto tab = line.find('\t');
    if (tab == std::string::npos) {
      continue;
    }
    kvs.push_back(slr::KeyValue{line.substr(0, tab), line.substr(tab + 1)});
  }
  return kvs;
}

long long to_int(const std::string& s) {
  try {
    return std::stoll(s);
  } catch (...) {
    return 0;
  }
}

}  // namespace

int main(int argc, char** argv) {
  std::vector<std::string> deployed_hosts;
  try {
    Config cfg = parse_args(argc, argv);

    const std::string root = scripts_root();
    const std::string worker_local = fs::weakly_canonical(fs::path(root) / "bin" / "slr_worker").string();
    if (!fs::exists(worker_local)) {
      throw std::runtime_error("missing worker binary: " + worker_local + " (run scripts/build_cpp.sh)");
    }

    std::string job_name = detect_job_name(cfg.job);
    (void)slr::parse_job_kind(job_name);

    auto hosts = slr::read_hosts(cfg.hosts_file, cfg.n);
    if (hosts.empty()) {
      throw std::runtime_error("no hosts available");
    }

    std::vector<std::string> peers;
    peers.reserve(hosts.size());
    for (const auto& h : hosts) {
      peers.push_back(h + ":" + std::to_string(cfg.port));
    }

    std::cerr << "deploying workers...\n";
    deploy_workers(hosts, worker_local, cfg.port, job_name, cfg.worker_pprof, cfg.parallel);
    deployed_hosts = hosts;

    std::cerr << "waiting health checks...\n";
    auto ready = health_ready(peers, 30, std::chrono::milliseconds(2000), cfg.parallel);
    if (ready.empty()) {
      throw std::runtime_error("no worker became healthy");
    }

    std::vector<std::string> ready_hosts;
    for (const auto& p : ready) {
      ready_hosts.push_back(p.substr(0, p.find(':')));
    }
    hosts = ready_hosts;
    peers = ready;

    if (cfg.n > 0 && cfg.n < static_cast<int>(peers.size())) {
      peers.resize(static_cast<size_t>(cfg.n));
      hosts.resize(static_cast<size_t>(cfg.n));
    }
    if (peers.empty()) {
      throw std::runtime_error("no active workers after applying -n");
    }

    std::cerr << "loading inputs...\n";
    if (cfg.commoncrawl) {
      auto urls = resolve_commoncrawl_urls(cfg.crawl, cfg.files_limit, cfg.chunks_limit);
      std::vector<std::string> per_peer(peers.size());
      for (size_t i = 0; i < urls.size(); ++i) {
        per_peer[i % peers.size()] += urls[i] + "\n";
      }
      slr::parallel_for(peers.size(), cfg.parallel, [&](size_t i) {
        post_payload(peers[i], "/load", per_peer[i]);
      });
    } else {
      auto chunks = split_chunks_round_robin(cfg.input_file, static_cast<int>(peers.size()));
      slr::parallel_for(peers.size(), cfg.parallel, [&](size_t i) {
        post_payload(peers[i], "/data", chunks[i]);
      });
    }

    std::string peer_lines;
    for (const auto& p : peers) {
      peer_lines += p + "\n";
    }

    std::cerr << "running map phase...\n";
    const auto compute_start = std::chrono::steady_clock::now();
    const auto map_start = compute_start;
    slr::parallel_for(peers.size(), cfg.parallel, [&](size_t i) {
      post_payload(peers[i], "/map", std::to_string(i) + "\n" + peer_lines);
    });
    const auto map_end = std::chrono::steady_clock::now();

    std::cerr << "running reduce phase...\n";
    const auto reduce_start = std::chrono::steady_clock::now();
    slr::parallel_for(peers.size(), cfg.parallel, [&](size_t i) {
      post_payload(peers[i], "/reduce", peer_lines);
    });
    const auto reduce_end = std::chrono::steady_clock::now();

    std::cerr << "collecting results...\n";
    const auto collect_start = std::chrono::steady_clock::now();
    std::map<std::string, long long> merged;
    for (const auto& peer : peers) {
      auto kvs = parse_result_lines(get_payload(peer, "/result"));
      for (const auto& kv : kvs) {
        merged[kv.key] += to_int(kv.value);
      }
    }
    const auto collect_end = std::chrono::steady_clock::now();

    std::vector<std::pair<std::string, long long>> entries;
    entries.reserve(merged.size());
    for (const auto& [k, v] : merged) {
      entries.emplace_back(k, v);
    }
    std::sort(entries.begin(), entries.end(), [](const auto& a, const auto& b) {
      if (a.second != b.second) {
        return a.second > b.second;
      }
      return a.first < b.first;
    });

    std::string out;
    for (const auto& [k, v] : entries) {
      out += k + "\t" + std::to_string(v) + "\n";
    }
    slr::write_file(cfg.output_file, out);
    std::cout << "results written to " << cfg.output_file << "\n";

    const auto secs = [](auto a, auto b) {
      return std::chrono::duration<double>(b - a).count();
    };
    char timing[256];
    std::snprintf(timing, sizeof(timing),
                  "TIMING nodes=%zu map_seconds=%.3f reduce_seconds=%.3f collect_seconds=%.3f compute_seconds=%.3f",
                  peers.size(), secs(map_start, map_end), secs(reduce_start, reduce_end),
                  secs(collect_start, collect_end), secs(compute_start, collect_end));
    std::cout << timing << "\n";

    cleanup_hosts(deployed_hosts, cfg.parallel);
    return 0;
  } catch (const std::exception& ex) {
    std::cerr << "error: " << ex.what() << "\n";
    if (!deployed_hosts.empty()) {
      cleanup_hosts(deployed_hosts, slr::kDefaultParallelism);
    }
    return 1;
  }
}
