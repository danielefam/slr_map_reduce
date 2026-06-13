#include "common.hpp"
#include "job.hpp"

#include <algorithm>
#include <arpa/inet.h>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
#include <netinet/in.h>
#include <sstream>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>

namespace fs = std::filesystem;

namespace {

struct State {
  std::mutex mu;
  std::string input_data;
  std::vector<std::string> input_files;
  int worker_id = 0;
  std::vector<std::string> peers;
  std::vector<slr::KeyValue> output;
};

std::string status_text(int code) {
  if (code == 200) {
    return "OK";
  }
  if (code == 400) {
    return "Bad Request";
  }
  if (code == 404) {
    return "Not Found";
  }
  return "Internal Server Error";
}

void send_response(int fd, int code, const std::string& body, const std::string& content_type = "text/plain; charset=utf-8") {
  std::ostringstream out;
  out << "HTTP/1.1 " << code << " " << status_text(code) << "\r\n";
  out << "Content-Type: " << content_type << "\r\n";
  out << "Content-Length: " << body.size() << "\r\n";
  out << "Connection: close\r\n\r\n";
  out << body;
  const std::string payload = out.str();
  size_t sent = 0;
  while (sent < payload.size()) {
    const ssize_t n = write(fd, payload.data() + sent, payload.size() - sent);
    if (n <= 0) {
      break;
    }
    sent += static_cast<size_t>(n);
  }
}

std::string run_cmd_or_throw(const std::string& cmd) {
  const auto result = slr::exec_capture(cmd);
  if (result.exit_code != 0) {
    throw std::runtime_error("command failed: " + cmd + "\n" + result.output);
  }
  return result.output;
}

std::string run_cmd_with_retries(const std::string& cmd, int attempts) {
  slr::CommandResult result{1, ""};
  for (int attempt = 1; attempt <= attempts; ++attempt) {
    result = slr::exec_capture(cmd);
    if (result.exit_code == 0) {
      return result.output;
    }
    if (attempt < attempts) {
      std::this_thread::sleep_for(std::chrono::milliseconds(500 * attempt));
    }
  }
  throw std::runtime_error("command failed: " + cmd + "\n" + result.output);
}

std::vector<std::string> split_body_lines(const std::string& body) {
  std::vector<std::string> out;
  for (const auto& line : slr::split_lines(body)) {
    if (!line.empty()) {
      out.push_back(line);
    }
  }
  return out;
}

std::map<std::string, std::vector<std::string>> run_map_text(const std::string& text, slr::JobKind job) {
  std::map<std::string, std::vector<std::string>> out;
  std::stringstream ss(text);
  std::string line;
  int line_no = 0;
  while (std::getline(ss, line)) {
    for (const auto& kv : slr::map_line(job, std::to_string(line_no), line)) {
      out[kv.key].push_back(kv.value);
    }
    ++line_no;
  }
  return out;
}

std::map<std::string, std::vector<std::string>> run_map_file(const std::string& path, slr::JobKind job) {
  std::string content;
  if (path.size() > 3 && path.substr(path.size() - 3) == ".gz") {
    content = run_cmd_or_throw("gzip -dc " + slr::shell_quote(path));
  } else {
    content = slr::read_file(path);
  }
  return run_map_text(content, job);
}

void write_bucket(const fs::path& dir, int n, int idx, const std::vector<slr::KeyValue>& kvs) {
  const fs::path out = dir / ("map-bucket-" + std::to_string(n) + "-" + std::to_string(idx) + ".tsv");
  std::ofstream f(out);
  if (!f) {
    throw std::runtime_error("failed to write bucket: " + out.string());
  }
  for (const auto& kv : kvs) {
    f << kv.key << '\t' << kv.value << '\n';
  }
}

std::vector<slr::KeyValue> read_bucket(const fs::path& dir, int n, int idx) {
  const fs::path in = dir / ("map-bucket-" + std::to_string(n) + "-" + std::to_string(idx) + ".tsv");
  std::vector<slr::KeyValue> out;
  if (!fs::exists(in)) {
    return out;
  }
  std::ifstream f(in);
  std::string line;
  while (std::getline(f, line)) {
    const auto tab = line.find('\t');
    if (tab == std::string::npos) {
      continue;
    }
    out.push_back(slr::KeyValue{line.substr(0, tab), line.substr(tab + 1)});
  }
  return out;
}

std::vector<slr::KeyValue> parse_kv_lines(const std::string& text) {
  std::vector<slr::KeyValue> out;
  std::stringstream ss(text);
  std::string line;
  while (std::getline(ss, line)) {
    const auto tab = line.find('\t');
    if (tab == std::string::npos) {
      continue;
    }
    out.push_back(slr::KeyValue{line.substr(0, tab), line.substr(tab + 1)});
  }
  return out;
}

std::map<std::string, std::vector<std::string>> merge_kvs(const std::vector<slr::KeyValue>& kvs) {
  std::map<std::string, std::vector<std::string>> out;
  for (const auto& kv : kvs) {
    out[kv.key].push_back(kv.value);
  }
  return out;
}

std::string extract_query_value(const std::string& query, const std::string& key) {
  const std::string needle = key + "=";
  const auto pos = query.find(needle);
  if (pos == std::string::npos) {
    return "";
  }
  const auto start = pos + needle.size();
  const auto end = query.find('&', start);
  if (end == std::string::npos) {
    return query.substr(start);
  }
  return query.substr(start, end - start);
}

void remove_input_files(const std::vector<std::string>& files) {
  for (const auto& file : files) {
    std::error_code ec;
    fs::remove(file, ec);
  }
}

void serve(int port, slr::JobKind job) {
  State state;
  const fs::path work_dir = fs::temp_directory_path() / ("slr-worker-" + std::to_string(::getpid()));
  fs::create_directories(work_dir);

  int server_fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) {
    throw std::runtime_error("socket() failed");
  }

  int opt = 1;
  setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = INADDR_ANY;
  addr.sin_port = htons(static_cast<uint16_t>(port));

  if (bind(server_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
    close(server_fd);
    throw std::runtime_error("bind() failed on port " + std::to_string(port));
  }
  if (listen(server_fd, 128) < 0) {
    close(server_fd);
    throw std::runtime_error("listen() failed");
  }

  std::cerr << "worker listening on :" << port << " (workDir=" << work_dir.string() << ")\n";

  while (true) {
    sockaddr_in client_addr{};
    socklen_t client_len = sizeof(client_addr);
    int client_fd = accept(server_fd, reinterpret_cast<sockaddr*>(&client_addr), &client_len);
    if (client_fd < 0) {
      continue;
    }

    std::thread([client_fd, &state, work_dir, job]() {
      try {
        std::string request;
        char buffer[4096];
        while (request.find("\r\n\r\n") == std::string::npos) {
          ssize_t n = read(client_fd, buffer, sizeof(buffer));
          if (n <= 0) {
            close(client_fd);
            return;
          }
          request.append(buffer, static_cast<size_t>(n));
          if (request.size() > (8 << 20)) {
            send_response(client_fd, 400, "request headers too large\n");
            close(client_fd);
            return;
          }
        }

        const auto header_end = request.find("\r\n\r\n");
        std::string headers = request.substr(0, header_end);
        std::string body = request.substr(header_end + 4);

        std::stringstream hs(headers);
        std::string request_line;
        std::getline(hs, request_line);
        if (!request_line.empty() && request_line.back() == '\r') {
          request_line.pop_back();
        }

        std::string method;
        std::string path;
        std::string version;
        {
          std::stringstream rl(request_line);
          rl >> method >> path >> version;
        }

        size_t content_length = 0;
        std::string line;
        while (std::getline(hs, line)) {
          if (!line.empty() && line.back() == '\r') {
            line.pop_back();
          }
          auto lower = line;
          std::transform(lower.begin(), lower.end(), lower.begin(), [](unsigned char c) {
            return static_cast<char>(std::tolower(c));
          });
          const std::string prefix = "content-length:";
          if (lower.rfind(prefix, 0) == 0) {
            content_length = static_cast<size_t>(std::stoul(slr::trim(line.substr(prefix.size()))));
          }
        }

        while (body.size() < content_length) {
          ssize_t n = read(client_fd, buffer, sizeof(buffer));
          if (n <= 0) {
            break;
          }
          body.append(buffer, static_cast<size_t>(n));
        }

        const auto qpos = path.find('?');
        const std::string route = (qpos == std::string::npos) ? path : path.substr(0, qpos);
        const std::string query = (qpos == std::string::npos) ? "" : path.substr(qpos + 1);

        if (method == "GET" && route == "/health") {
          send_response(client_fd, 200, "ok\n");
          close(client_fd);
          return;
        }

        if (method == "POST" && route == "/data") {
          std::vector<std::string> stale;
          {
            std::lock_guard<std::mutex> lock(state.mu);
            stale = state.input_files;
            state.input_files.clear();
            state.input_data = body;
            state.output.clear();
          }
          remove_input_files(stale);
          send_response(client_fd, 200, "ok\n");
          close(client_fd);
          return;
        }

        if (method == "POST" && route == "/load") {
          std::vector<std::string> stale;
          {
            std::lock_guard<std::mutex> lock(state.mu);
            stale = state.input_files;
            state.input_files.clear();
            state.input_data.clear();
            state.output.clear();
          }
          remove_input_files(stale);

          const auto urls = split_body_lines(body);
          std::vector<std::string> paths;
          paths.reserve(urls.size());
          for (size_t i = 0; i < urls.size(); ++i) {
            const fs::path out = work_dir / ("cc-input-" + std::to_string(i) + ".wet.gz");
            std::string cmd = "curl -fsSL " + slr::shell_quote(urls[i]) + " -o " + slr::shell_quote(out.string());
            run_cmd_or_throw(cmd);
            paths.push_back(out.string());
          }

          {
            std::lock_guard<std::mutex> lock(state.mu);
            state.input_files = std::move(paths);
          }
          send_response(client_fd, 200, "ok\n");
          close(client_fd);
          return;
        }

        if (method == "POST" && route == "/map") {
          auto lines = split_body_lines(body);
          if (lines.empty()) {
            send_response(client_fd, 400, "missing map payload\n");
            close(client_fd);
            return;
          }
          int worker_id = std::stoi(lines.front());
          std::vector<std::string> peers(lines.begin() + 1, lines.end());
          if (peers.empty()) {
            send_response(client_fd, 400, "missing peers\n");
            close(client_fd);
            return;
          }

          std::string input_data;
          std::vector<std::string> input_files;
          {
            std::lock_guard<std::mutex> lock(state.mu);
            state.worker_id = worker_id;
            state.peers = peers;
            state.output.clear();
            input_data = state.input_data;
            input_files = state.input_files;
          }

          for (const auto& file : fs::directory_iterator(work_dir)) {
            if (file.path().filename().string().find("map-bucket-") == 0) {
              std::error_code ec;
              fs::remove(file.path(), ec);
            }
          }

          std::map<std::string, std::vector<std::string>> intermediate;
          if (!input_files.empty()) {
            for (const auto& p : input_files) {
              auto per_file = run_map_file(p, job);
              for (auto& [k, vals] : per_file) {
                auto& dst = intermediate[k];
                dst.insert(dst.end(), vals.begin(), vals.end());
              }
            }
            remove_input_files(input_files);
            {
              std::lock_guard<std::mutex> lock(state.mu);
              state.input_files.clear();
            }
          } else {
            intermediate = run_map_text(input_data, job);
          }

          const int n = static_cast<int>(peers.size());
          std::vector<std::vector<slr::KeyValue>> buckets(static_cast<size_t>(n));
          for (const auto& [key, values] : intermediate) {
            const int idx = slr::target_node(key, n);
            for (const auto& v : values) {
              buckets[static_cast<size_t>(idx)].push_back(slr::KeyValue{key, v});
            }
          }
          for (int i = 0; i < n; ++i) {
            write_bucket(work_dir, n, i, buckets[static_cast<size_t>(i)]);
          }
          send_response(client_fd, 200, "ok\n");
          close(client_fd);
          return;
        }

        if (method == "GET" && route == "/intermediate") {
          const std::string reducer_s = extract_query_value(query, "reducer");
          const std::string n_s = extract_query_value(query, "n");
          if (reducer_s.empty() || n_s.empty()) {
            send_response(client_fd, 400, "need reducer and n\n");
            close(client_fd);
            return;
          }
          int reducer = std::stoi(reducer_s);
          int n = std::stoi(n_s);
          if (n <= 0 || reducer < 0 || reducer >= n) {
            send_response(client_fd, 400, "invalid reducer/n\n");
            close(client_fd);
            return;
          }

          const auto kvs = read_bucket(work_dir, n, reducer);
          std::string out;
          for (const auto& kv : kvs) {
            out += kv.key + "\t" + kv.value + "\n";
          }
          send_response(client_fd, 200, out);
          close(client_fd);
          return;
        }

        if (method == "POST" && route == "/reduce") {
          auto lines = split_body_lines(body);
          std::vector<std::string> peers;
          int worker_id = 0;
          {
            std::lock_guard<std::mutex> lock(state.mu);
            peers = state.peers;
            worker_id = state.worker_id;
          }
          if (!lines.empty()) {
            peers = lines;
            std::lock_guard<std::mutex> lock(state.mu);
            state.peers = peers;
          }
          if (peers.empty()) {
            send_response(client_fd, 400, "no peers\n");
            close(client_fd);
            return;
          }

          std::vector<slr::KeyValue> all;
          for (const auto& peer : peers) {
            const std::string cmd = "curl -fsSL " + slr::shell_quote("http://" + peer + "/intermediate?reducer=" + std::to_string(worker_id) + "&n=" + std::to_string(peers.size()));
            const std::string text = run_cmd_with_retries(cmd, 3);
            auto part = parse_kv_lines(text);
            all.insert(all.end(), part.begin(), part.end());
          }

          auto grouped = merge_kvs(all);
          std::vector<slr::KeyValue> output;
          output.reserve(grouped.size());
          for (auto& [key, values] : grouped) {
            output.push_back(slr::KeyValue{key, slr::reduce_values(job, key, values)});
          }
          std::sort(output.begin(), output.end(), [](const slr::KeyValue& a, const slr::KeyValue& b) {
            return a.key < b.key;
          });

          {
            std::lock_guard<std::mutex> lock(state.mu);
            state.output = std::move(output);
          }
          send_response(client_fd, 200, "ok\n");
          close(client_fd);
          return;
        }

        if (method == "GET" && route == "/result") {
          std::vector<slr::KeyValue> output;
          {
            std::lock_guard<std::mutex> lock(state.mu);
            output = state.output;
          }
          std::sort(output.begin(), output.end(), [](const slr::KeyValue& a, const slr::KeyValue& b) {
            return a.key < b.key;
          });
          std::string out;
          for (const auto& kv : output) {
            out += kv.key + "\t" + kv.value + "\n";
          }
          send_response(client_fd, 200, out);
          close(client_fd);
          return;
        }

        send_response(client_fd, 404, "not found\n");
      } catch (const std::exception& ex) {
        send_response(client_fd, 500, std::string("error: ") + ex.what() + "\n");
      }
      close(client_fd);
    }).detach();
  }
}

}  // namespace

int main(int argc, char** argv) {
  int port = 9090;
  std::string job_name = "wordcount";

  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "-port" && i + 1 < argc) {
      port = std::stoi(argv[++i]);
    } else if (arg == "-job" && i + 1 < argc) {
      job_name = argv[++i];
    } else if (arg == "-pprof") {
      // Kept for CLI compatibility with previous worker; no-op in C++ worker.
    } else {
      std::cerr << "Usage: slr_worker [-port 9090] [-job wordcount|langdetect|domainpop|docdensity] [-pprof]\n";
      return 1;
    }
  }

  try {
    const auto job = slr::parse_job_kind(job_name);
    serve(port, job);
  } catch (const std::exception& ex) {
    std::cerr << "error: " << ex.what() << "\n";
    return 1;
  }

  return 0;
}
