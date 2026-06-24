---
marp: true
theme: default
paginate: true
size: 16:9
---

<!--
10-minute target:
1. Title and promise: 0:30
2. Problem/context: 1:00
3. Architecture: 1:15
4. Protocol/dataflow: 1:15
5. Fault tolerance: 1:00
6. Use cases: 1:00
7. Results/Amdahl: 1:15
8. Kafka comparison: 0:50
9. Missing items and one-week plan: 1:10
10. Demo/Q&A framing: 0:45
-->

# Distributed MapReduce on Lab Machines

SLR Project P4

Custom Go MapReduce framework running on `*.enst.fr` workers, with direct Common Crawl input, pluggable jobs, local-disk shuffle, and fault-aware coordination.

<!--
Say: Our goal was not to reproduce Hadoop completely. It was to build and understand a working distributed MapReduce system under lab constraints: shared machines, SSH access, no root, no Docker, and NFS pressure.
-->

---

# What We Built

- Automatic host discovery from `https://tp.telecom-paris.fr/ajax.php`
- Go orchestrator that builds, deploys, starts, monitors, and cleans workers
- HTTP worker API for `load -> map -> shuffle -> reduce -> result`
- Direct Common Crawl WET download from `data.commoncrawl.org`
- Four jobs: `wordcount`, `langdetect`, `domainpop`, `docdensity`

**Current verification:** 89 Go tests passed, live 100-host discovery passed, small reference wordcount matched exactly.

<!--
Say: The most important result is that the core distributed computation is real: workers map local input, write reducer buckets, fetch buckets from peers, reduce, and return outputs.
-->

---

# Architecture

```text
User shell
  |
  v
mapreduce.sh
  |-- make_hosts -> hosts.txt
  |
  v
Go orchestrator
  |-- build worker with selected job
  |-- SCP + SSH start workers
  |-- health checks and retries
  |-- load / map / reduce / collect
  v
Remote workers on lab machines
  /health /data /load /map /intermediate /reduce /result
```

Design choice: the client is the Main. Workers are stateless between jobs except for local temporary files under `/tmp`.

<!--
Say: This is intentionally simple. There is no permanent cluster service. Each run selects machines, builds the worker with the selected job, starts workers, runs the job, then cleans up.
-->

---

# Protocol and Dataflow

1. **Load**
   Local file chunks via `/data`, or Common Crawl WET URLs via `/load`.
2. **Map**
   Each worker writes `N` bucket files on local disk.
3. **Shuffle + reduce**
   Reducer `i` pulls bucket `i` from every peer via `/intermediate`.
4. **Collect**
   Main fetches `/result`, merges integer values, sorts, writes final output.

Reducer assignment:

```text
FNV-32a(key) % N
```

<!--
Say: The key property is that partitioning is done at map time. During reduce, each worker can directly serve the bucket requested by the reducer, so there is no full scan of all intermediate data during shuffle.
-->

---

# Fault Tolerance and Operations

- Logical **slots** stay fixed: `0..N-1`
- Physical hosts can be replaced by cold spares
- Health watcher polls `/health` and marks repeated failures dead
- Failed slots replay prerequisites: `load -> map -> reduce`
- If a peer dies during reduce, coordinator re-runs stale reduces with the new peer list
- Cleanup is idempotent: kill PID if present, remove worker files, tolerate repeated cleanup

**Tested behavior:** coordinator replacement, peer reduce failure, cold spare activation, fallback to surviving worker.

<!--
Say: The important idea is separating logical worker slots from physical machines. Hash partitioning depends on N, so N stays stable while the host behind a slot can change.
-->

---

# Use Cases Beyond Wordcount

| Job | What it measures | Example output |
| --- | --- | --- |
| `wordcount` | token frequency | `the 239298` |
| `langdetect` | language signal counts | `lang:en`, `lang:fr`, `lang:unknown` |
| `domainpop` | crawled domain popularity | `doi.org`, `youtube.com` |
| `docdensity` | content size and density | `stat:lines`, `hist:len.*` |

All reducers emit integer values so the orchestrator can safely merge partial outputs.

<!--
Say: The three additional jobs demonstrate that this is a framework, not only a wordcount script. They reuse the same deployment, shuffle, reduce, and collect path.
-->

---

# Measurements and Amdahl's Law

![width:900px](report/amdahl_8.png)

Observed scaling on the fixed Common Crawl workload:

- `1 node`: 206.95 s, speedup 1.0
- `16 nodes`: 40.80 s, speedup 5.07
- `64 nodes`: 39.06 s, speedup 5.30

Interpretation: after the useful parallelism is exhausted, communication, synchronization, and final merge dominate.

<!--
Say: The curve is a good teaching result. Speedup improves, then flattens. At high N the worker mesh and reduce traffic become the bottleneck, so simply adding machines does not give linear speedup.
-->

---

# Batch vs Kafka Streams

| Custom MapReduce | Kafka Streams |
| --- | --- |
| Batch job with explicit phases | Continuous stream processing model |
| Lightweight HTTP + local temp files | Persistent Kafka log and state stores |
| Fast on small bounded workloads | More operationally robust at scale |
| No HDFS-style replication | Built around durable broker storage |

Kafka path is implemented without Docker or root: KRaft deployment in `/tmp`, Java Streams wordcount, timing by consumer-group lag.

<!--
Say: Kafka is not just a faster or slower version of our system. It is a different model. It pays persistence and broker overhead, but gains durability and a mature streaming runtime.
-->

---

# What Is Missing and One-Week Plan

We will present the core system now, then harden evidence and compatibility next week.

| Gap | One-week fix |
| --- | --- |
| Exact `1 SCP to HOME + 100 SSH` rubric mode | Add compatibility deploy mode or document design choice |
| Port collision preflight | Remote port check before starting workers |
| Standalone MapReduce cleanup script | Add `clean_mapreduce.sh` using host file |
| `/cal/commoncrawl` evidence | Add input mode or document direct Amazon path as replacement |
| Local storage proof | Save `df` / `mount` evidence for lab machines |
| Large remote proof | Run and save 100-node / larger-split logs |
| Stragglers and atomic final writes | Add speculative task note or implementation, temp+rename outputs |

<!--
Say: These are mostly operational and evidence gaps. The core MapReduce protocol, jobs, and recovery model already work and are tested. We are not hiding the missing items; we are converting them into a concrete next-week work plan.
-->

---

# Demo and Discussion Plan

**Live demo path**

```bash
(cd scripts && go test ./...)
./mapreduce.sh -job scripts/jobs/wordcount -input test_input.txt -n 4 -output result.txt
./run_all_commoncrawl_jobs.sh -crawl CC-MAIN-2026-21 -files-limit 1 -n 4
```

**If lab machines are unstable**

- Show passed tests and local reference validation
- Show checked-in Common Crawl outputs and timing CSVs
- Show Amdahl plot and explain the bottleneck

**Suggested speaking split**

- Speaker 1: problem, architecture, protocol
- Speaker 2: jobs, results, Kafka comparison
- Speaker 3: fault tolerance, missing items, roadmap, Q&A

<!--
Say: End by inviting questions about the tradeoffs: why no HDFS, why direct Common Crawl, why local /tmp, why speedup plateaus, and what we would fix next.
-->
