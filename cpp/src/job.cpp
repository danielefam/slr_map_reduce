#include "job.hpp"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <cstdint>
#include <map>
#include <set>
#include <stdexcept>
#include <string_view>
#include <unordered_map>

namespace slr {
namespace {

std::string lower_copy(const std::string& s) {
  std::string out = s;
  std::transform(out.begin(), out.end(), out.begin(), [](unsigned char c) {
    return static_cast<char>(std::tolower(c));
  });
  return out;
}

std::vector<std::string> split_words_alnum(const std::string& text) {
  std::vector<std::string> out;
  std::string cur;
  for (unsigned char ch : text) {
    if (std::isalnum(ch)) {
      cur.push_back(static_cast<char>(std::tolower(ch)));
    } else if (!cur.empty()) {
      out.push_back(cur);
      cur.clear();
    }
  }
  if (!cur.empty()) {
    out.push_back(cur);
  }
  return out;
}

bool starts_with(const std::string& s, const std::string& prefix) {
  return s.size() >= prefix.size() && s.compare(0, prefix.size(), prefix) == 0;
}

int atoi_or_zero(const std::string& s) {
  int value = 0;
  const auto* begin = s.data();
  const auto* end = s.data() + s.size();
  auto [ptr, ec] = std::from_chars(begin, end, value);
  if (ec != std::errc{} || ptr != end) {
    return 0;
  }
  return value;
}

bool is_warc_header(const std::string& line) {
  return starts_with(line, "WARC/") || starts_with(line, "WARC-") ||
         starts_with(line, "Content-Type:") || starts_with(line, "Content-Length:");
}

std::string strip_punct_edges(const std::string& token) {
  size_t b = 0;
  size_t e = token.size();
  while (b < e && std::ispunct(static_cast<unsigned char>(token[b]))) {
    ++b;
  }
  while (e > b && std::ispunct(static_cast<unsigned char>(token[e - 1]))) {
    --e;
  }
  return token.substr(b, e - b);
}

std::string host_from_url(const std::string& raw) {
  std::string s = raw;
  const auto scheme = s.find("://");
  if (scheme != std::string::npos) {
    s = s.substr(scheme + 3);
  }
  const auto slash = s.find('/');
  if (slash != std::string::npos) {
    s = s.substr(0, slash);
  }
  const auto at = s.rfind('@');
  if (at != std::string::npos) {
    s = s.substr(at + 1);
  }
  const auto colon = s.find(':');
  if (colon != std::string::npos) {
    s = s.substr(0, colon);
  }
  s = lower_copy(s);
  if (starts_with(s, "www.")) {
    s = s.substr(4);
  }
  return s;
}

const std::map<std::string, std::set<std::string>> kStopWords = {
    {"en", {"the", "and", "of", "to", "in", "is", "that", "it", "for", "was", "with", "are", "this", "have", "not", "they", "from", "you", "which"}},
    {"fr", {"le", "la", "les", "de", "des", "du", "et", "est", "que", "qui", "dans", "pour", "une", "sur", "pas", "avec", "sont", "mais", "nous"}},
    {"de", {"der", "die", "das", "und", "ist", "von", "den", "nicht", "mit", "sich", "auf", "ein", "eine", "auch", "als", "wird", "sind", "dem"}},
    {"es", {"el", "la", "los", "las", "de", "que", "y", "en", "es", "por", "una", "con", "para", "del", "se", "su", "como", "mas", "pero"}},
    {"it", {"il", "la", "di", "che", "e", "per", "un", "una", "del", "della", "con", "non", "sono", "anche", "come", "alla", "gli", "nel"}},
};

std::string length_bucket(int n) {
  if (n < 80) {
    return "00000-00079";
  }
  if (n < 200) {
    return "00080-00199";
  }
  if (n < 500) {
    return "00200-00499";
  }
  if (n < 1000) {
    return "00500-00999";
  }
  if (n < 5000) {
    return "01000-04999";
  }
  return "05000-plus";
}

}  // namespace

JobKind parse_job_kind(const std::string& raw) {
  const std::string v = lower_copy(raw);
  if (v == "wordcount") {
    return JobKind::kWordCount;
  }
  if (v == "langdetect") {
    return JobKind::kLangDetect;
  }
  if (v == "domainpop") {
    return JobKind::kDomainPop;
  }
  if (v == "docdensity") {
    return JobKind::kDocDensity;
  }
  throw std::runtime_error("unsupported job: " + raw);
}

std::vector<KeyValue> map_line(JobKind job, const std::string&, const std::string& text) {
  switch (job) {
    case JobKind::kWordCount: {
      std::vector<KeyValue> out;
      for (const auto& w : split_words_alnum(text)) {
        out.push_back(KeyValue{w, "1"});
      }
      return out;
    }
    case JobKind::kLangDetect: {
      std::string line = text;
      line.erase(line.begin(), std::find_if(line.begin(), line.end(), [](unsigned char c) { return !std::isspace(c); }));
      line.erase(std::find_if(line.rbegin(), line.rend(), [](unsigned char c) { return !std::isspace(c); }).base(), line.end());
      if (line.empty() || is_warc_header(line)) {
        return {};
      }
      auto tokens = split_words_alnum(line);
      if (tokens.size() < 3) {
        return {};
      }
      std::string best = "unknown";
      int best_score = 0;
      for (const auto& [lang, words] : kStopWords) {
        int score = 0;
        for (const auto& tok : tokens) {
          if (words.find(tok) != words.end()) {
            ++score;
          }
        }
        if (score > best_score) {
          best = lang;
          best_score = score;
        }
      }
      return {KeyValue{"lang:" + best, "1"}};
    }
    case JobKind::kDomainPop: {
      std::string line = text;
      line.erase(line.begin(), std::find_if(line.begin(), line.end(), [](unsigned char c) { return !std::isspace(c); }));
      line.erase(std::find_if(line.rbegin(), line.rend(), [](unsigned char c) { return !std::isspace(c); }).base(), line.end());
      if (line.empty()) {
        return {};
      }
      constexpr std::string_view kPrefix = "WARC-Target-URI:";
      if (starts_with(line, std::string(kPrefix))) {
        std::string url = line.substr(kPrefix.size());
        url = strip_punct_edges(lower_copy(url));
        const std::string host = host_from_url(url);
        if (host.empty()) {
          return {};
        }
        return {KeyValue{host, "1"}};
      }
      if (starts_with(line, "WARC/") || starts_with(line, "WARC-")) {
        return {};
      }
      std::vector<KeyValue> out;
      std::string token;
      for (char c : line) {
        if (std::isspace(static_cast<unsigned char>(c))) {
          if (!token.empty()) {
            std::string t = strip_punct_edges(token);
            if (starts_with(t, "http://") || starts_with(t, "https://")) {
              const std::string host = host_from_url(t);
              if (!host.empty()) {
                out.push_back(KeyValue{host, "1"});
              }
            }
            token.clear();
          }
        } else {
          token.push_back(c);
        }
      }
      if (!token.empty()) {
        std::string t = strip_punct_edges(token);
        if (starts_with(t, "http://") || starts_with(t, "https://")) {
          const std::string host = host_from_url(t);
          if (!host.empty()) {
            out.push_back(KeyValue{host, "1"});
          }
        }
      }
      return out;
    }
    case JobKind::kDocDensity: {
      std::string line = text;
      line.erase(line.begin(), std::find_if(line.begin(), line.end(), [](unsigned char c) { return !std::isspace(c); }));
      line.erase(std::find_if(line.rbegin(), line.rend(), [](unsigned char c) { return !std::isspace(c); }).base(), line.end());
      if (line.empty() || is_warc_header(line)) {
        return {};
      }

      int total = 0;
      int alnum = 0;
      for (unsigned char c : line) {
        ++total;
        if (std::isalnum(c)) {
          ++alnum;
        }
      }
      return {
          KeyValue{"stat:lines", "1"},
          KeyValue{"stat:chars.total", std::to_string(total)},
          KeyValue{"stat:chars.alnum", std::to_string(alnum)},
          KeyValue{"stat:chars.other", std::to_string(total - alnum)},
          KeyValue{"hist:len." + length_bucket(total), "1"},
      };
    }
  }
  return {};
}

std::string reduce_values(JobKind job, const std::string&, const std::vector<std::string>& values) {
  if (job == JobKind::kDocDensity) {
    long long sum = 0;
    for (const auto& v : values) {
      sum += static_cast<long long>(atoi_or_zero(v));
    }
    return std::to_string(sum);
  }
  return std::to_string(values.size());
}

int target_node(const std::string& key, int n) {
  constexpr uint32_t kOffset = 2166136261u;
  constexpr uint32_t kPrime = 16777619u;
  uint32_t hash = kOffset;
  for (unsigned char c : key) {
    hash ^= c;
    hash *= kPrime;
  }
  return static_cast<int>(hash % static_cast<uint32_t>(n));
}

}  // namespace slr
