#pragma once

#include <string>
#include <vector>

namespace slr {

struct KeyValue {
  std::string key;
  std::string value;
};

enum class JobKind {
  kWordCount,
  kLangDetect,
  kDomainPop,
  kDocDensity,
};

JobKind parse_job_kind(const std::string& raw);

std::vector<KeyValue> map_line(JobKind job, const std::string& doc_id, const std::string& text);
std::string reduce_values(JobKind job, const std::string& key, const std::vector<std::string>& values);

int target_node(const std::string& key, int n);

}  // namespace slr
