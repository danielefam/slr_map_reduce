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
| Worker | `bin/slr_worker` (one HTTP server per node, port 9090) | executes map/reduce, stores intermediate buckets on node-local disk, serves peers |
| Jobs | built-in C++ job names (`wordcount`, `langdetect`, `domainpop`, `docdensity`) | selected via `-job <name>` |
| Host discovery | `bin/slr_make_hosts` | queries `tp.telecom-paris.fr` for available lab machines → `hosts.txt` |

### 2.2 Job lifecycle

1. **Build:** `scripts/build_cpp.sh` compiles C++ binaries (`slr_worker`, `slr_mapreduce`, and helper tools).
2. **Deploy:** the binary is `scp`-ed to `/tmp/mr-worker` on every host and
   started with `nohup` (PID recorded in `/tmp/mr-worker.pid`); operations are
   bounded by `-parallel` (default 16) concurrent SSH/SCP sessions.
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

**Design constraint:** the orchestrator's final merge sums values as integers
(`mergeResults` does `strconv.Atoi` + `+=`). All four reducers therefore emit
integer strings; `docdensity` emits *summable counters* (totals + histogram
buckets) and derives averages/ratios offline, instead of emitting floats.
_Alternative considered:_ a per-job merge strategy (e.g. a `Merger` interface
in `mrjob` with `sum` as the default) would support non-integer outputs such
as top-K lists; rejected for the final delivery as it changes the public job
API for no current use case.

---

## 3. Protocol Design ✅

### 3.1 Worker HTTP API

| Endpoint | Method | Purpose |
| -------- | ------ | ------- |
| `/health` | GET | readiness + liveness probe |
| `/data` | POST | receive a raw input chunk (local-file mode) |
| `/load` | POST | download assigned Common Crawl WET URLs |
| `/map` | POST | run map; body `{"id": i, "peers": [...]}` fixes slot identity and N |
| `/intermediate?reducer=i&n=N` | GET | serve the pre-partitioned bucket for reducer *i* |
| `/reduce` | POST | pull buckets from all peers, run reduce; optional body updates the peer list after a replacement |
| `/result` | GET | final `key\tvalue` lines, sorted by key |
| `/debug/pprof/*` | GET | profiling endpoints (only with `-pprof`, see §6) |

### 3.2 Partitioning: FNV-32a

Every intermediate key is routed to reducer `FNV-32a(key) mod N`. FNV-32a is
allocation-free, fast on short keys, and — critically — **deterministic across
processes and machines**, so every mapper independently computes the same
key→reducer assignment with zero coordination. N is fixed for the whole job
(it is baked into the partition function), which is exactly why fault
tolerance replaces *hosts behind slots* instead of changing N (§4).

### 3.3 The O(1) pull-based shuffle

At the end of the map phase each worker writes its intermediate pairs into
**N pre-partitioned bucket files** (`map-bucket-<N>-<idx>.jsonl`) on
node-local disk. When reducer *i* asks any peer for its data, the peer answers
`GET /intermediate?reducer=i&n=N` by streaming exactly one file — **no
scanning, no filtering, no index lookup**: O(1) file opens per request, O(N²)
total fetches for the whole shuffle, each carrying only the data the reducer
actually needs.

Pull-based (reducers fetch when ready) rather than push-based (mappers send
eagerly) keeps the protocol idempotent: a replacement reducer can simply
re-fetch all its buckets, and a re-run map phase atomically overwrites its
bucket files before any peer reads them.

🔲 _Include the sequence diagram from `diagrams/mapreduce-pipeline.seq`._

---

## 4. Fault Tolerance: the Slot-Based Coordinator ✅

### 4.1 Concepts

- **Slot** — one of the N logical workers (0…N−1). The FNV partition depends
  on N, so slots are immutable; the *physical host* serving a slot is not.
- **Spare** — a host that deployed successfully but holds no slot. Healthy
  spares are the replacement pool.
- **Phase state machine** — per slot: `pending → loaded → mapped → reduced →
  done` (`sFailed` terminal). A replacement host re-plays all prerequisite
  phases (re-load its input, re-map) before rejoining the current phase.
- **Epoch** — a counter bumped on every host swap. Because reducers pull from
  *all* peers, a swap invalidates every completed reduce: slots in `reduced`
  state are demoted to `mapped` and re-run their reduce against the updated
  peer list (sent in the `POST /reduce` body).

### 4.2 Failure detection and attribution

1. **Active health watcher** — a background goroutine polls `GET /health` on
   every active host (default every 5 s); two consecutive failures mark the
   host dead and cancel its in-flight requests.
2. **Transport errors** — a failed `POST` to a slot's own host triggers
   retry with exponential backoff (250 ms doubling, capped at 5 s, max
   `-max-attempts`) and eventually replacement.
3. **Peer-failure attribution** — if a reduce fails with
   `fetch from peer X: …`, the coordinator parses the failing *peer* from the
   error and replaces **X**, not the reporting worker.

When no spare is available for a dead slot host, the job fails fast with a
clear error instead of hanging.

🔲 _Add the fault-injection experiment: kill a worker during map and during
reduce; show the orchestrator log lines (`[slot i] replaced A -> B … epoch
now k`) and the unchanged final output checksum._

---

## 5. Evaluation

### 5.1 Experimental setup

🔲 _Fill in: number/spec of lab machines, dataset (crawl ID, WET file count,
total bytes), Go version, repetitions per point, how compute time is defined
(map+reduce+collect; `/data`–`/load` excluded — quote the `TIMING` log line)._

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
  `kafka/run_kafka_wordcount.sh` on the **same input file** the Go framework
  processed; both print a `TIMING compute_seconds=…` line (see
  `kafka/README.md` for what each timer covers).
- Report a table: system × N × median compute_seconds (≥ 3 runs).
- Discuss the asymmetry honestly: Kafka persists every record to a replicated
  log (durability, exactly-once options, incremental results) while our
  framework is a volatile batch system; explain why that makes Kafka slower
  per record but operationally stronger, and when each model wins.

---

## 6. Performance Audit & Bottlenecks ✅ (findings) / 🔲 (profiles)

Profiling support: workers expose `/debug/pprof/` when started with `-pprof`
(orchestrator flag `-worker-pprof`). Capture during a run:

```bash
go tool pprof "http://<host>:9090/debug/pprof/profile?seconds=30"   # CPU
go tool pprof "http://<host>:9090/debug/pprof/heap"                 # memory
```

Audit findings (code reading; confirm each with a pprof capture):

1. **Map-phase memory (highest impact).** `runMap` materialises
   `map[string][]string` with one `"1"` string *per token occurrence*, then
   `handleMap` builds a second full copy in per-reducer bucket slices before
   writing. Peak memory ≈ 2× the expanded intermediate data; for word-count
   this dwarfs the input itself. _Proposal:_ in-mapper combining (count into
   `map[string]int` for jobs whose reduce is associative) and streaming bucket
   writes (append to the N bucket files as lines are mapped) would cut peak
   memory by an order of magnitude.
2. **Shuffle JSON overhead.** `/intermediate` reads the whole bucket file into
   a `[]KeyValue`, then re-encodes it as one JSON array; the fetching side
   decodes the full array into memory. Each shuffled byte is deserialized and
   reserialized once on each side plus held twice in RAM. _Proposal:_ stream
   the `.jsonl` file directly with `io.Copy` (the format on disk is already
   line-delimited JSON) and decode line-by-line on the reducer; this makes the
   transfer zero-copy on the serving side.
3. **Buffer over-allocation.** Every `runMap`/`readKVFlat` call allocates a
   64 MB scanner buffer up front, even for tiny buckets — N buckets fetched
   concurrently in reduce allocate N × 64 MB transiently.
4. **NFS exposure is already avoided (good).** Intermediate buckets and WET
   downloads live in `os.MkdirTemp` (node-local `/tmp`), not NFS home; the
   only NFS writes are the orchestrator-side output file. No change needed,
   but worth stating in the report.
5. **Serial final merge.** `mergeResults` and the output sort run
   single-threaded in the orchestrator — part of the serial fraction measured
   in §5.2.

🔲 _Insert flame graphs / `pprof top` tables for one map-heavy and one
reduce-heavy run proving items 1–3._

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
