# Criteria Audit Report

Audit date: 2026-06-24  
Project: SLR Project P4 - distributed MapReduce

## Status Legend

- PASS: implemented and evidenced by code, tests, run artifacts, or a fresh verification command.
- PARTIAL: some requirement is met, but an important detail is missing, undocumented, inconsistent, or not freshly re-run.
- MISSING: no convincing evidence in the current source tree or run artifacts.
- NOT EVIDENCED: the project might satisfy it operationally, but this repository does not prove it.

## Fresh Verification Performed

The following checks were run during this audit:

- Focused Go test run through the test tool: 70 tests passed, 0 failed. Covered worker protocol, hash partitioning, Common Crawl URL resolution, coordinator recovery, and the three extra jobs.
- Follow-up broader Go test run through the test tool: 89 tests passed, 0 failed. This added coverage for worker HTTP behavior, generated worker startup, Common Crawl load/download cleanup, multi-worker shuffle/reduce, and all job packages.
- Live host API query: `cd scripts && go run ./make_hosts -n 100 -f /tmp/slr_hosts_live_100.txt` returned 100 hosts.
- Live host validation: `/tmp/slr_hosts_live_100.txt` had count 100, 0 non-`.enst.fr` entries, and 0 duplicates.
- Repository host validation: `hosts_100.txt` had count 100, 0 non-`.enst.fr` entries, and 0 duplicates.
- Small wordcount reference check: independent `awk` wordcount over `test_input.txt` matched `result.txt` exactly (`diff` returned no differences).
- Port-collision probe with the real worker binary: first worker became healthy on a free local port; a second worker on the same port exited with `listen tcp :<port>: bind: address already in use`.
- Local `/cal/commoncrawl` check: `/cal/commoncrawl` is not present in this environment.
- Live Common Crawl direct-read check: fetched `https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-21/wet.paths.gz`; first WET URL resolved with HTTP 200, `content-length: 64504740`, `content-type: application/octet-stream`.
- Kafka archive check: cached `kafka_2.13-4.3.0.tgz` contains `bin/kafka-server-start.sh` and `libs/kafka-streams-4.3.0.jar`; local `javac` is Java 8, so local Kafka 4.3 compilation cannot be validated on this machine.
- Local microbenchmarks ran successfully: worker `BenchmarkRunMap` about 17.7 ms/op, worker `BenchmarkRunReduce` about 9.1 ms/op, `BenchmarkSplitIntoChunksMedium` about 0.43 ms/op, and `BenchmarkCollectResults` about 3.85 ms/op for the benchmark fixtures.

Remote deployment across 100 machines was not re-run during this audit. Where a criterion depends on a large remote run, I separate implementation evidence from current execution proof.

## Executive Summary

The strongest parts of the project are the current Go MapReduce core, protocol, direct Common Crawl input path, local-disk intermediate storage, fault-aware coordinator, and extra analytical jobs. The worker pipeline is well tested and the checked-in small and Common Crawl outputs give useful evidence.

The main gaps are operational/reporting details around the original infrastructure spec: the current deployment is not the requested single SCP to NFS HOME plus 100 SSH starts; there is no explicit port-collision preflight; there is no proof of local storage exploration beyond `/tmp`; the `/cal/commoncrawl` NFS path is not used; there is no evidenced 100 or 1000+ node MapReduce run; straggler backup tasks are not implemented; Hadoop/HDFS comparison is thin; and no demo/member work-distribution artifact was found.

## 1. Infrastructure and Deployment

| Criterion | Status | Evidence and analysis |
| --- | --- | --- |
| [Base] List of 100 live machines generated automatically using `ajax.php`, canonical names ending in `.enst.fr` | PASS | `scripts/make_hosts/main.go` calls `https://tp.telecom-paris.fr/ajax.php`, parses entries whose availability boolean is true, and writes `name + ".enst.fr"`. Fresh query to `/tmp/slr_hosts_live_100.txt` returned 100 unique canonical hosts. `hosts_100.txt` also contains 100 unique `.enst.fr` names. |
| [Base] Deployment script equals 1 SCP to HOME (NFS) plus 100 SSH starts | MISSING | The current Go deployment does per-host SCP to `/tmp/mr-worker.deploy-<timestamp>` via `deployWorker` and `scpTo` in `scripts/mapreduce/main.go`, then starts each worker over SSH. There is no current source evidence for one shared SCP to `$HOME`/NFS followed by 100 SSH starts. `hosts_ssh100.txt` and `hosts_scp100.txt` are empty. |
| [Base] Servers listen on a chosen port, with no collision with an already-used port | PARTIAL | `mapreduce.sh` and `scripts/mapreduce/main.go` expose `-port` and start workers with that port. Active local probe: one real worker became healthy on a chosen free port; a second worker on the same port failed fast with `bind: address already in use`. This proves collisions are detected by the OS/server startup, but I found no proactive preflight or automatic collision-free port selection before remote launch. |
| [Base] Cleaning script kills all deployed servers; clean redeployment possible | PARTIAL | The orchestrator defers cleanup and `cleanupWorkers` kills `/tmp/mr-worker.pid`, removes `/tmp/mr-worker.log`, `/tmp/mr-worker`, and `/tmp/mr-worker-*`. Deployment also kills an old PID before moving the new binary. This supports clean redeployments, but I found no standalone MapReduce cleaning script equivalent to `kafka/clean_kafka.sh` in the current source tree. |
| [Solid] Deployment robust to unreachable machines (timeout, no blocking) | PASS | SSH/SCP helpers in `scripts/internal/remote/remote.go` use `context.WithTimeout`, `ConnectTimeout=10`, server-alive settings, and `BatchMode=yes`. `deployInitialWorkers` skips failed startup candidates and continues until it fills the target slots or exhausts the host pool. The implementation is sequential for initial deploy, but bounded by timeouts. |
| [Solid] SSH with keys and fingerprint bypass configured | PASS | `scripts/internal/remote/remote.go` sets `BatchMode=yes` (no password prompts), `StrictHostKeyChecking=no` (fingerprint prompt bypass), and connection reuse options. |
| [Solid] Deployment and cleaning scripts idempotent and replayable | PASS | `deployWorker` kills any existing PID and removes stale files before installing the new worker. `cleanupWorkers` uses `kill ... || true`, `rm -f`, and `rm -rf`, so repeated cleanup is tolerated. Kafka cleanup also uses tolerant kill/remove operations. |

## 2. NFS vs Local Disk

| Criterion | Status | Evidence and analysis |
| --- | --- | --- |
| [Base] You know the HOME is on NFS, not local disk | PARTIAL | The design clearly avoids NFS by using `/tmp`, and `kafka/README.md` explicitly says Kafka lives on node-local `/tmp` and never NFS. Follow-up search still found no checked-in `mount`, `df`, or similar proof that `$HOME` is NFS on the lab machines. |
| [Solid] Map intermediate files are written to local disk, not NFS | PASS | Workers create `workDir` with `os.MkdirTemp("", "mr-worker-*")`, which defaults to `/tmp` on Linux. Bucket files are built under `workDir` by `bucketFile`, and reduce peer files/sorted files are also under `workDir`. |
| [Solid] Deliberately limit load on the NFS | PASS | Current MapReduce deployment uses remote `/tmp`, workers download Common Crawl WET files into local `workDir`, map from local streams, delete downloaded inputs after mapping, and exchange intermediate buckets by HTTP directly between workers. This avoids repeated writes to shared HOME/NFS. |
| [Advanced] Explored local storage beyond `/tmp` using `df`/`mount` and use it appropriately | MISSING | Follow-up storage search found `/tmp`/NFS documentation and code paths, but still no `df`, `mount`, scratch-partition discovery, or alternate local-storage selection logic. The implementation standardizes on `/tmp`. |

## 3. Protocol Design

| Criterion | Status | Evidence and analysis |
| --- | --- | --- |
| [Base] Main / Workers roles clearly defined | PASS | `README.md` and `docs/REPORT.md` define the orchestrator/main and worker roles. The orchestrator builds/deploys/coordinates/merges; workers expose HTTP endpoints and run map/reduce. |
| [Base] Space-time diagram of V1 done with sequencediagram.org | PARTIAL | `README.md` contains Mermaid sequence diagrams for the pipeline and intermediate pull detail. I found no `sequencediagram.org` source or export. The diagram requirement is functionally covered, but not in the specified tool format. |
| [Solid] Synchronization of map -> shuffle -> reduce phases working | PASS | `coordinator.drive` advances slots through `sLoaded`, `sMapped`, `sReduced`, and `sDone`. Reduce pulls peer bucket files via `/intermediate`. Tests cover single-worker, multi-worker, concurrent reduce, empty workers, and coordinator happy path. |
| [Solid] System bootstrap handled cleanly | PASS | The orchestrator validates flags, reads hosts, cross-compiles the worker with injected job binding, deploys, starts via SSH, waits on `/health`, then assigns inputs. Startup failures are skipped when possible. |
| [Advanced] Protocol formalized and documented (messages, formats, edge cases) | PASS | `README.md`, `docs/REPORT.md`, and `docs/JOBS.md` document endpoints, request bodies, hash partitioning, job contract, Common Crawl mode, timing, fault behavior, and failure cases. Worker code enforces query/body formats. |

## 4. MapReduce Core

| Criterion | Status | Evidence and analysis |
| --- | --- | --- |
| [Base] Word frequency works on small splits | PASS | Worker and job tests cover tokenization, map, reduce, single-worker and multi-worker pipelines. Fresh independent reference comparison showed `result.txt` exactly matches `test_input.txt`. |
| [Base] Key distribution at reduce via `hash(key) mod N` | PASS | `scripts/worker/mapreduce.go` uses FNV-32a in `targetNode(key, n)` and writes to `idx := targetNode(kv.Key, n)`. Tests verify determinism, range, and bucket correctness through `/intermediate`. |
| [Solid] Remote read of local intermediate files for reduce phase | PASS | `/intermediate` serves a pre-partitioned local bucket file. `fetchIntermediate` performs HTTP GETs from each peer and writes fetched data into the reducer's local `workDir` before `sort` and reduce. |
| [Solid] Results validated against a single-machine reference on the same small dataset | PASS | Fresh `awk` reference counts over `test_input.txt` matched `result.txt` exactly. Unit tests also validate expected counts for small inputs. |
| [Advanced] Works on real Common Crawl splits at scale (100, 1000+) | PARTIAL | The project has real Common Crawl runs and outputs in `run/commoncrawl_jobs`, with timing CSVs for 4-node runs and a LaTeX report table for 1 to 64 nodes. Active follow-up verified the live CC-MAIN-2026-21 WET manifest and first WET file over HTTPS. However, I found no evidenced MapReduce run at 100 nodes or 1000+ splits. Existing run logs show one WET URL distributed across four slots for the checked-in Common Crawl job artifacts. |

## 5. Common Crawl Data

| Criterion | Status | Evidence and analysis |
| --- | --- | --- |
| [Base] Reading splits from `/cal/commoncrawl` | MISSING | Active local check: `/cal/commoncrawl` is not present in this environment. Source/code search found no implementation reading that path. The current implementation resolves WET paths from `https://data.commoncrawl.org` and has workers download URLs directly in `/load`. |
| [Solid] Understanding of the structure of Common Crawl | PASS | The code resolves `wet.paths.gz`, uses WET files, handles `.gz`, and jobs inspect WARC/WET metadata such as `WARC-Target-URI`, `WARC-` headers, `Content-Type`, and `Content-Length`. |
| [Advanced] Direct read from Amazon without internal NFS | PASS | `scripts/mapreduce/main.go` uses `commonCrawlDataURL = "https://data.commoncrawl.org"`; workers download selected WET URLs directly into local temp files. Active follow-up fetched the live `wet.paths.gz` manifest and confirmed the first generated WET URL returns HTTP 200 from `data.commoncrawl.org`. This bypasses internal NFS. |

## 6. Performance and Amdahl's Law

| Criterion | Status | Evidence and analysis |
| --- | --- | --- |
| [Base] Timing of every step (deployment, cleaning, I/O, sync/waiting, network, compute) | PARTIAL | The orchestrator emits `TIMING nodes=... map_seconds=... reduce_seconds=... collect_seconds=... compute_seconds=...`. `run_all_commoncrawl_jobs.sh` records these per job. Follow-up local microbenchmarks also produced fresh timings for map, reduce, chunk split, and collect/merge functions. Deployment and cleanup are logged but not consistently timed in the current MapReduce path; I/O, synchronization/waiting, and network are not separated. `experiments/run_deploy_benchmark.sh` exists but references legacy commands not present in the current source listing. |
| [Solid] Bottlenecks identified and backed by measurements | PASS | `docs/REPORT.md` identifies map memory, shuffle serialization, scanner buffers, and serial final merge. `docs/report/report.tex` interprets the high-node plateau as communication/synchronization/serial merge overhead. Benchmark scripts record compute seconds and speedup. |
| [Solid] Reference point established (speedup = 1 for 1 node) | PASS | `experiments/run_benchmark.sh` computes speedup from the N=1 median. `docs/report/report.tex` includes a table with 1 node = 206.95 s and speedup = 1.0000. |
| [Solid] Speedup vs number of nodes graph with same dataset at every point | PASS | `experiments/run_benchmark.sh` fixes the workload across node counts; `experiments/amdahl.gnuplot` plots measured speedup and Amdahl fit. `docs/report/amdahl_8.png` and `docs/report/amdahl_8_loglog.png` are checked in. |
| [Advanced] Knows when projection/interpolation is acceptable and when not | PARTIAL | The benchmark scripts avoid extrapolation by measuring each N and discarding runs with the wrong active node count. However, I found no explicit written rule explaining when projection/interpolation is acceptable versus forbidden. |
| [Advanced] Amdahl's law verified empirically and interpreted | PASS | `docs/report/report.tex` includes measured 1, 2, 4, 8, 16, 32, 64 node timings, speedups, Amdahl plots, and interpretation that speedup plateaus/regresses as communication, synchronization, and serial merge dominate. |

## 7. Fault Tolerance

| Criterion | Status | Evidence and analysis |
| --- | --- | --- |
| [Base] Detection of a dead worker (ping / timeout) | PASS | Coordinator has `watchHealth`, `pollHealth`, `/health` checks, HTTP timeouts, and marks hosts dead after repeated health failures. Startup uses health probes before accepting workers. |
| [Solid] Re-execution of lost tasks by the Main | PASS | The slot state machine resets replacement slots to pending, replays load/map/reduce as needed, and bumps an epoch so stale reduces are rerun. Tests cover map transport failure, peer reduce failure, cold spare activation, and fallback to a surviving worker. |
| [Solid] Atomic output writes (temporary file + rename) | PARTIAL | Follow-up write-path search confirmed Common Crawl downloads are atomic (`os.CreateTemp` then `os.Rename`). Worker bucket files use direct `OpenFile`, fetched peer files use `os.Create`, and final merged outputs use `os.WriteFile` directly. The criterion is only partially met. |
| [Advanced] Demonstration by killing nodes mid-computation | PARTIAL | Unit tests simulate dead workers and peer failures, but I found no remote demo artifact or log showing actual lab nodes killed mid-computation. |
| [Advanced] Handling of stragglers (backup tasks) | MISSING | The coordinator handles failures and retries, but there is no backup-task/speculative execution logic for slow-but-alive stragglers. |

## 8. Comparison and Kafka Streams

| Criterion | Status | Evidence and analysis |
| --- | --- | --- |
| [Base] Can situate batch vs stream | PASS | `kafka/README.md` and `docs/report/report.tex` describe Kafka Streams as an unbounded stream-processing baseline and the custom framework as batch MapReduce. |
| [Solid] Minimal wordcount with Kafka Streams, downloaded files, no Docker/root | PASS | `kafka/deploy_kafka.sh` deploys Kafka KRaft into `/tmp` without root/Docker, verifies Java 17, and starts brokers. `kafka/WordCountJob.java` implements Streams wordcount and self timing. `kafka/run_kafka_wordcount.sh` compiles/runs using bundled Kafka libs. Follow-up archive inspection confirmed the cached Kafka 4.3 distribution contains `kafka-streams-4.3.0.jar` and server startup scripts. Local compile was not possible because this machine has Java 8, while Kafka 4.3 requires Java 17; checked-in Kafka result files remain the run evidence. |
| [Solid] Documented comparison vs Hadoop and Kafka Streams, including missing HDFS/features | PARTIAL | Kafka comparison is documented. I found little explicit Hadoop/HDFS comparison in the current docs, and no clear checklist of simplified-system gaps versus Hadoop beyond durability/replication discussion. |
| [Advanced optional] Direct Common Crawl read via Kafka source/connector | MISSING | `kafka/run_kafka_wordcount.sh` consumes a local input file after external staging. `kafka/commoncrawl_direct_result.txt` exists but is empty. No Kafka source connector or direct Common Crawl source was found. |

## 9. Use Cases, Report, and Demo

| Criterion | Status | Evidence and analysis |
| --- | --- | --- |
| [Base] Three use cases beyond wordcount | PASS | Implemented jobs: `scripts/jobs/langdetect`, `scripts/jobs/domainpop`, and `scripts/jobs/docdensity`. Unit tests and checked-in outputs exist for all three. |
| [Solid] Use case results interpreted and explained | PASS | `docs/REPORT.md` explains the three jobs, local validation outputs, and derived docdensity metrics. Common Crawl outputs under `run/commoncrawl_jobs` provide additional results, though they could use deeper interpretation. |
| [Base] Report covers architecture, protocol, metrics, Amdahl graph, pain points, 3 use cases, batch/stream comparison | PARTIAL | `docs/REPORT.md` covers architecture/protocol/use cases/metrics/pain points/Kafka, while `docs/report/report.tex` covers Amdahl graph and batch/stream comparison. The coverage exists across documents, but the two reports contain inconsistencies about benchmark status and dataset size. |
| [Base] Demo ready (10 min presentation + 10 min questions), work distributed between members | MISSING | I found no slide deck, demo script, timing plan, Q&A preparation, or member work-distribution artifact. `docs/report/report.tex` lists one author only. |

## Key Risks and Recommended Fixes

1. Restore or document the exact deployment strategy expected by the course. If the grading rubric requires one SCP to NFS HOME plus 100 SSH starts, the current per-host `/tmp` SCP design does not satisfy that exact base item.
2. Add a remote port preflight before worker start. For example, run a small remote check that fails when the selected port is already listening, then choose or ask for a new port.
3. Add a standalone MapReduce cleanup script that reads a host file and kills/removes workers from every touched host, independent of orchestrator defer cleanup.
4. Record NFS and local storage evidence: run and save `df -h`, `mount`, and any lab scratch partition discovery. If `/tmp` is the only intended local store, document that explicitly.
5. Decide whether `/cal/commoncrawl` is required. If yes, add that input mode. If direct Common Crawl is preferred, state it as an intentional advanced replacement and explain the tradeoff.
6. Fix atomic writes for final output and bucket writes where practical: write to a temp file in the same directory and `rename` into place.
7. Add a small fault-tolerance demo script/log that kills a remote worker during map or reduce and shows slot replacement and successful final output.
8. Add explicit Hadoop/HDFS comparison: missing HDFS replication, durable task tracker/history, speculative execution, data locality scheduling, and mature resource management.
9. Add demo artifacts: 10-minute script, command checklist, expected outputs, and member responsibility split.

## Overall Verdict

The project is solid as a custom Go batch MapReduce system with direct Common Crawl input, a useful Kafka Streams comparison path, good unit coverage, and credible fault-recovery mechanics. It is weaker as a match for the original infrastructure/deployment rubric, especially where the rubric expects NFS HOME deployment, `/cal/commoncrawl`, explicit port collision avoidance, large 100/1000+ scale evidence, straggler handling, and demo logistics.
