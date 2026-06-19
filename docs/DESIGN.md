# Design & Implementation Notes

## C++ Implementation Choices

This project is written in **C++20** for performance-critical path scheduling and
local multi-threading. Key design decisions:

### Compiled-in Jobs

The worker binary embeds all four jobs and selects one at runtime with `-job`:

- `wordcount` — Classic word count (lowercase, alphanumeric tokens)
- `langdetect` — Per-document language guess via stop-word scoring
- `domainpop` — Domain popularity from WARC headers
- `docdensity` — Document/word statistics and histograms

Adding a new job means updating `cpp/src/job.hpp` and `cpp/src/job.cpp`, then
rebuilding the worker.

### Plain-Text Line Protocol

Worker HTTP endpoints accept and return newline-separated text/TSV, parsed
without a JSON library:

- `POST /data` — raw text chunk
- `POST /load` — WET URLs, one per line
- `POST /map` — worker id, then peers (one per line)
- `GET /intermediate` — TSV buckets
- `POST /reduce` — peer list
- `GET /result` — final TSV (`key\tvalue` lines)

This keeps every step debuggable with `curl` and avoids a heavy JSON dependency.

### Shell Delegation

Heavy lifting is delegated to shell commands:

- HTTP client calls use `curl` (avoids a C++ HTTP library)
- JSON manifest parsing uses `jq` (avoids a JSON library)
- Decompression uses `gzip` (avoids a compression library)

The C++ code remains lean and dependency-free, while orchestration scripts
handle system-level concerns.

### Concurrency Model

- **Per-connection threading**: worker accepts each HTTP connection on a
  detached `std::thread`
- **Parallel operations**: a simple `parallel_for` helper schedules SSH/SCP
  fan-out with bounded concurrency (default 16 hosts)
- **Exception propagation**: worker threads report errors back to the main
  thread via exception capture; deployment failures are collected and reported

### Remote-Compatible Worker Builds

The orchestrator runs locally but must deploy a worker binary to lab hosts
running an older glibc/libstdc++:

1. Local build produces `bin/slr_worker` for the laptop's runtime
2. `mapreduce.sh` checks if `bin/slr_worker_remote` is stale or missing
3. If so, it compiles the worker on the first selected lab host using
   `scripts/build_remote_worker.sh` over SSH
4. The lab-compatible binary is then deployed to all other hosts

This avoids glibc/libstdc++ version mismatches and keeps the orchestrator's
build environment local.

## MapReduce Shuffle Architecture

Keys are partitioned using **FNV-32a hashing**:

```
reducer_index = fnv32a(key) % N
```

where `N` is the active worker count. This assignment is **fixed for the entire
job**, so a given key always lands on the same reducer across all chunks.

**Map phase** writes one bucket file per reducer per chunk:
```
map-bucket-{reducer}-{chunk}.tsv
```

**Reduce phase** is pull-based: each worker *i* fetches its buckets from all
peers via `GET /intermediate?reducer=i&n=N`, then groups and reduces locally.
No orchestrator involvement during reduce — workers coordinate directly.

## Fault Tolerance

- **Health polling** during long phases (map/reduce/collect) detects dead hosts
- **Slot replacement** from spare pool if a worker dies mid-job
- **Retries with backoff** for transient SSH/HTTP failures
- **Cleanup on exit** removes remote files (also on Ctrl-C via EXIT trap)

If spare pool is exhausted or a single slot trips max retries, the job fails.
