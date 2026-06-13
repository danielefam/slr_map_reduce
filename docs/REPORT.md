# SLR Project P4 — Distributed MapReduce: Final Report

> **Status:** skeleton — sections marked ✅ are pre-filled from the implementation;
> sections marked 🔲 contain instructions for content still to be produced.

**Authors:** _<fill in>_
**Date:** _<fill in>_

---

## 1. Introduction

🔲 _Write 2–3 paragraphs:_
- Goal: a from-scratch distributed MapReduce framework in C++ over the
  `*.enst.fr` lab pool, benchmarked on Common Crawl data and compared to
  Apache Kafka Streams.
- Constraints: no root, no Docker, NFS home directories, shared machines.
- Summary of headline results (fill in after benchmarks).

---

## 2. Architecture of the System ✅

### 2.1 Components

| Component | Location | Role |
| --------- | -------- | ---- |
| Orchestrator | `bin/slr_mapreduce` (runs on the local machine) | deploys workers, splits input, drives phases, merges results, cleans up |
| Worker | `bin/slr_worker_remote` on lab nodes (`bin/slr_worker` for local tests) | executes map/reduce, stores intermediate buckets on node-local disk, serves peers |
| Jobs | built-in C++ job names (`wordcount`, `langdetect`, `domainpop`, `docdensity`) | selected via `-job <name>` |
| Host discovery | `bin/slr_make_hosts` | queries `tp.telecom-paris.fr` for available lab machines → `hosts.txt` |

### 2.2 Job lifecycle

1. **Build:** `scripts/build_cpp.sh` compiles local C++ binaries (`slr_worker`, `slr_mapreduce`, and helper tools). `scripts/build_remote_worker.sh` compiles the deployed worker on a lab host so the binary matches the lab glibc/libstdc++ runtime.
2. **Deploy:** the lab-compatible worker is `scp`-ed to a user-scoped path in
   `/tmp` on every host and started with `nohup` (PID/log files use matching
   user-scoped names); operations are bounded by `-parallel` (default 16)
   concurrent SSH/SCP sessions.
3. **Input assignment:** local files are split into ≤ 64 MB chunks on newline
   boundaries (`POST /data`); in `-commoncrawl` mode WET URLs are resolved from
   the official index and assigned round-robin (`POST /load`, workers download
   directly — the orchestrator never touches the payload).
4. **Phases:** `load → map → reduce → result`, each driven to completion for
   all N slots before the next phase starts (§3).
5. **Merge:** per-key integer sums, sorted by descending count then key.
6. **Cleanup:** `ssh kill` on every deployed host, even on failure paths
   (deferred in the orchestrator).

🔲 _Insert the architecture diagram here (adapt `diagrams/mapreduce-pipeline.seq`)._

### 2.3 Pluggable jobs (use cases)

All jobs are selected at run time via `./mapreduce.sh -job <name> …`:

| Job | Key → Value | Question answered |
| --- | ----------- | ----------------- |
| `wordcount` | word → occurrences | reference job |
| `langdetect` | `lang:<code>` → line count | language distribution via stop-word scoring (en/fr/de/es/it + unknown) |
| `domainpop` | domain → page count | domain popularity from `WARC-Target-URI` WET headers |
| `docdensity` | `stat:*` / `hist:len.*` → sums | average line length, alphanumeric density, length histogram |

**Design constraint:** the orchestrator's final merge sums values as integers.
All four reducers therefore emit integer strings; `docdensity` emits
*summable counters* (totals + histogram buckets) and derives averages/ratios
offline, instead of emitting floats. A future C++ extension could add per-job
merge policies, but the current implementation deliberately keeps the job
contract small: `map_line` emits key/value pairs and `reduce_values` returns a
single value for a key.

---

## 3. Protocol Design ✅

### 3.1 Worker HTTP API

| Endpoint | Method | Purpose |
| -------- | ------ | ------- |
| `/health` | GET | readiness + liveness probe |
| `/data` | POST | receive a raw input chunk (local-file mode) |
| `/load` | POST | download assigned Common Crawl WET URLs |
| `/map` | POST | run map; body is plain text: worker id on the first line, then one peer per line |
| `/intermediate?reducer=i&n=N` | GET | serve the pre-partitioned bucket for reducer *i* |
| `/reduce` | POST | pull buckets from all peers and run reduce; body is the peer list, one peer per line |
| `/result` | GET | final `key\tvalue` lines, sorted by key |

### 3.2 Partitioning: FNV-32a

Every intermediate key is routed to reducer `FNV-32a(key) mod N`. FNV-32a is
allocation-free, fast on short keys, and — critically — **deterministic across
processes and machines**, so every mapper independently computes the same
key→reducer assignment with zero coordination. N is fixed for the whole job.

### 3.3 The O(1) pull-based shuffle

At the end of the map phase each worker writes its intermediate pairs into
**N pre-partitioned bucket files** (`map-bucket-<reducer>-<idx>.tsv`) on
node-local disk. When reducer *i* asks any peer for its data, the peer answers
`GET /intermediate?reducer=i&n=N` by streaming exactly one file — **no
scanning, no filtering, no index lookup**: O(1) file opens per request, O(N²)
total fetches for the whole shuffle, each carrying only the data the reducer
actually needs.

Pull-based (reducers fetch when ready) rather than push-based (mappers send
eagerly) keeps the worker protocol simple and debuggable with `curl`.

🔲 _Include the sequence diagram from `diagrams/mapreduce-pipeline.seq`._

---

## 4. Operational Robustness ✅ / 🔲

The C++ orchestrator is intentionally simpler than a production fault-tolerant
coordinator. It performs bounded SSH/SCP fan-out, waits for `GET /health`,
keeps only workers that become ready, and cleans up deployed workers on both
success and failure. During a job, a failed phase request stops the run with a
clear error instead of producing partial output.

Important robustness work completed during migration:

- **Remote ABI compatibility:** local g++ 13/glibc 2.38 binaries do not run on
   the Debian lab nodes (glibc 2.36). `scripts/build_remote_worker.sh` now
   compiles the deployed worker on a lab host with g++ 12 and copies back
   `bin/slr_worker_remote`.
- **Shared `/tmp` collisions:** worker executable, PID, and log paths are
   user-scoped under `/tmp`, avoiding permission conflicts with stale files
   created by other users or previous runs.
- **Worker availability check:** the orchestrator retries health probes before
   sending input.

Future work: spare-host replacement during map/reduce, reduce replay after a
peer swap, and per-host retry/backoff around transient SSH/SCP failures.

---

## 5. Evaluation

### 5.1 Experimental setup

Initial validation run completed on 2026-06-13:

```bash
./mapreduce.sh -job wordcount -commoncrawl -files-limit 3 -chunks-limit 3 \
   -output result_commoncrawl_3files_3nodes.txt -n 3 -port 9193
```

Result: 3 lab nodes, 3 Common Crawl WET files, 2,094,920 distinct output keys,
27 MB result file. Timing reported by the C++ orchestrator:

```text
TIMING nodes=3 map_seconds=23.671 reduce_seconds=18.409 collect_seconds=4.567 compute_seconds=46.647
```

`compute_seconds` covers map + reduce + collect; `/data` and `/load` input
transfer are excluded.

### 5.2 Amdahl's Law analysis

🔲 _Instructions:_
- Run `experiments/run_benchmark.sh` for N ∈ {1, 2, 4, 8, 16, …} on a fixed
  Common Crawl workload; collect `compute_seconds` from the `TIMING` lines.
- Plot speedup S(N) = T(1)/T(N) with `experiments/amdahl.gnuplot`; fit the
  serial fraction and overlay the ideal Amdahl curve.
- Discuss: which phases scale (map), which do not (collect/merge is serial in
  the orchestrator; the shuffle is O(N²) requests), where the knee is, and the
  effect of input skew (round-robin URL assignment vs. file-size imbalance).

### 5.3 Comparison with Kafka Streams

🔲 _Instructions:_
- Deploy with `kafka/deploy_kafka.sh -n <same N>`; run
   `kafka/run_kafka_wordcount.sh` on the **same input file** the C++ framework
  processed; both print a `TIMING compute_seconds=…` line (see
  `kafka/README.md` for what each timer covers).
- Report a table: system × N × median compute_seconds (≥ 3 runs).
- Discuss the asymmetry honestly: Kafka persists every record to a replicated
  log (durability, exactly-once options, incremental results) while our
  framework is a volatile batch system; explain why that makes Kafka slower
  per record but operationally stronger, and when each model wins.

---

## 6. Performance Audit & Bottlenecks ✅ (findings) / 🔲 (profiles)

The C++ worker currently exposes no built-in profiler endpoint. Capture CPU or
memory profiles with system tools (`/usr/bin/time -v`, `perf` if permitted on
the lab image, or periodic `/proc/<pid>/status` sampling over SSH).

Audit findings (code reading; confirm each with a profile):

1. **Common Crawl load memory.** `/load` downloads and decompresses WET files
   through `curl | gzip -dc` and stores the resulting text in worker memory.
   For larger file counts, streaming map directly from the decompressor would
   reduce peak memory.
2. **Map intermediate size.** `map_line` emits one key/value pair per token for
   `wordcount`. An in-mapper combiner for associative jobs could turn repeated
   words into local counts before bucket writes.
3. **Shuffle fan-out.** Reduce performs O(N²) HTTP fetches (`N` reducers × `N`
   peers). This is simple and correct but becomes visible at high N.
4. **NFS exposure is avoided.** Intermediate buckets and WET downloads live in
   node-local `/tmp`, not NFS home; the orchestrator writes only the final
   output file locally.
5. **Serial final merge.** Result merge and output sort run single-threaded in
   the orchestrator — part of the serial fraction measured in §5.2.

🔲 _Insert profile tables for one map-heavy and one reduce-heavy run proving
items 1–3._

---

## 7. Pain Points & Lessons Learned

🔲 _Bullet the war stories with evidence (log excerpts):_
- NFS home directories vs. node-local state (permissions, `scp` failures,
  stale PID files).
- Non-blocking sockets / shuffle debugging on multi-worker reduce.
- Shared machines: load variance forcing median-of-runs methodology.
- Fixing N at job start vs. elastic reducer counts (FNV partition trade-off).
- Anything encountered during the Kafka deployment without root.

---

## 8. Conclusion

🔲 _Summarise: what was built, headline speedup numbers, Kafka comparison
verdict, and 2–3 future-work items (streaming shuffle, per-job mergers,
combiner support)._

---

## Appendix A — Reproducing every result

```bash
# Word count on Common Crawl, 10 nodes
./mapreduce.sh -job wordcount -commoncrawl -files-limit 10 -n 10 -output wc.txt

# The three analysis jobs
./mapreduce.sh -job langdetect -commoncrawl -files-limit 10 -n 10 -output lang.txt
./mapreduce.sh -job domainpop  -commoncrawl -files-limit 10 -n 10 -output domains.txt
./mapreduce.sh -job docdensity -commoncrawl -files-limit 10 -n 10 -output density.txt

# docdensity post-processing (averages/ratios from integer counters)
awk -F'\t' '$1=="stat:chars.total"{t=$2} $1=="stat:chars.alnum"{a=$2} $1=="stat:lines"{l=$2}
            END{printf "avg line len = %.1f chars\nalnum density = %.3f\n", t/l, a/t}' density.txt

# Kafka comparison
(cd kafka && ./deploy_kafka.sh -n 10 && ./run_kafka_wordcount.sh -input /tmp/cc_split.txt && ./clean_kafka.sh)
```
