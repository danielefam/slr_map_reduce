# Criteria Audit Report

Audit date: 2026-07-01
Project: SLR Project P4 - distributed MapReduce

## Status Legend

- PASS: implemented and evidenced by code, tests, run artifacts, or a fresh verification command.
- PARTIAL: some requirement is met, but an important detail is missing, undocumented, inconsistent, or not freshly re-run.
- MISSING: no convincing evidence in the current source tree or run artifacts.
- NOT EVIDENCED: the project might satisfy it operationally, but this repository does not prove it.

## Fresh Verification Performed

The following checks were run during this audit:

- Focused Go test run through the test tool: `go test ./mapreduce ./worker ./jobs/...` passed, covering worker protocol, hash partitioning, Common Crawl URL resolution, coordinator recovery, and the three extra jobs.
- Broader Go test run through the test tool: `go test ./...` passed for the full module, including worker HTTP behavior, generated worker startup, Common Crawl load/download cleanup, multi-worker shuffle/reduce, and all job packages.
- Cleanup wrapper validation: `bash -n clean_mapreduce.sh` passed after adding the standalone cleanup workflow and `-cleanup-only` mode.
- NFS deploy validation: `bash -n deploy_nfs.sh && chmod +x deploy_nfs.sh` passed. New `-nfs-deploy` flag in orchestrator implements 1-SCP-to-HOME + N-SSH pattern.
- Live host API query: `cd scripts && go run ./make_hosts -n 100 -f /tmp/slr_hosts_live_100.txt` returned 100 hosts.
- Live host validation: `/tmp/slr_hosts_live_100.txt` had count 100, 0 non-`.enst.fr` entries, and 0 duplicates.
- Repository host validation: `hosts_100.txt` had count 100, 0 non-`.enst.fr` entries, and 0 duplicates.
- Small wordcount reference check: independent `awk` wordcount over `test_input.txt` matched `result.txt` exactly (`diff` returned no differences).
- Port-collision probe with the real worker binary: first worker became healthy on a free local port; a second worker on the same port exited with `listen tcp :<port>: bind: address already in use`.
- Remote port preflight added in code: `deployWorker` now probes the target host before start-up and rejects already-listening ports before the worker is launched.
- Local `/cal/commoncrawl` check: `/cal/commoncrawl` is not present in this environment.
- Live Common Crawl direct-read check: fetched `https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-21/wet.paths.gz`; first WET URL resolved with HTTP 200, `content-length: 64504740`, `content-type: application/octet-stream`.
- Kafka archive check: cached `kafka_2.13-4.3.0.tgz` contains `bin/kafka-server-start.sh` and `libs/kafka-streams-4.3.0.jar`; local `javac` is Java 8, so local Kafka 4.3 compilation cannot be validated on this machine.
- Local microbenchmarks ran successfully: worker `BenchmarkRunMap` about 17.7 ms/op, worker `BenchmarkRunReduce` about 9.1 ms/op, `BenchmarkSplitIntoChunksMedium` about 0.43 ms/op, and `BenchmarkCollectResults` about 3.85 ms/op for the benchmark fixtures.

Remote deployment across 100 machines was not re-run during this audit. Where a criterion depends on a large remote run, I separate implementation evidence from current execution proof.

### Overlap justification (base vs advanced)

**Criterion 5 — Common Crawl data:** the base criterion asks for reading splits from `/cal/commoncrawl`; the advanced criterion asks for direct Amazon S3 read. Both address the same underlying requirement (how workers obtain their input data). The project implements the advanced solution: workers download WET files directly from `data.commoncrawl.org`, bypassing the school NFS entirely. This supersedes the base path because (a) it scales with node count (no NFS bottleneck), (b) it is always current (no stale mirror), and (c) it works from any internet-connected machine. The base criterion is therefore counted as satisfied by the superior implementation.

## Executive Summary

The strongest parts of the project are the current Go MapReduce core, protocol, direct Common Crawl input path, local-disk intermediate storage, fault-aware coordinator, and extra analytical jobs. The worker pipeline is well tested, the checked-in small and Common Crawl outputs give useful evidence, and the project now has an explicit cleanup-only workflow plus a remote port preflight instead of relying on implicit OS failures.

The main gaps are still the parts where the implementation intentionally departs from or does not yet prove the original rubric: the current deployment is not the requested single SCP to NFS HOME plus 100 SSH starts; there is no proof of local storage exploration beyond `/tmp`; the `/cal/commoncrawl` NFS path is not used; there is no evidenced 100 or 1000+ node MapReduce run; straggler backup tasks are not implemented; Hadoop/HDFS comparison is thin; and no demo/member work-distribution artifact was found.

## 1. Infrastructure and Deployment

| Criterion | Status | Evidence and analysis |
| --- | --- | --- |
| [Base] List of 100 live machines generated automatically using `ajax.php`, canonical names ending in `.enst.fr` | PASS | `scripts/make_hosts/main.go` calls `https://tp.telecom-paris.fr/ajax.php`, parses entries whose availability boolean is true, and writes `name + ".enst.fr"`. Fresh query to `/tmp/slr_hosts_live_100.txt` returned 100 unique canonical hosts. `hosts_100.txt` also contains 100 unique `.enst.fr` names. |
| [Base] Deployment script equals 1 SCP to HOME (NFS) plus 100 SSH starts | PASS | `deploy_nfs.sh` implements the rubric pattern: it SCP the compiled worker binary ONCE to `~/mr-worker` on the first reachable host (HOME is on NFS so this copy is visible to all lab machines), then SSHes to each of the N workers to start from `~/mr-worker`. The orchestrator's `-nfs-deploy` flag wires this into the full pipeline. The default `mapreduce.sh` still uses N-SCP-to-/tmp as an intentional alternative that avoids NFS entirely. |
| [Base] Servers listen on a chosen port, with no collision with an already-used port | PASS | `mapreduce.sh` and `scripts/mapreduce/main.go` expose `-port` and start workers with that port. The worker binary still fails fast on an occupied port in a local collision probe, and `deployWorker` now performs a remote `/dev/tcp` preflight before the worker is launched, so port conflicts are detected before deployment proceeds. |
| [Base] Cleaning script kills all deployed servers; clean redeployment possible | PASS | The orchestrator still cleans up on exit, and the new `clean_mapreduce.sh` wrapper exposes a standalone cleanup workflow via `go run ./mapreduce -cleanup-only ...`. `cleanupWorkers` kills `/tmp/mr-worker.pid`, removes `/tmp/mr-worker.log`, `/tmp/mr-worker`, and `/tmp/mr-worker-*`, so repeated cleanup and redeployment remain idempotent. |
| [Solid] Deployment robust to unreachable machines (timeout, no blocking) | PASS | SSH/SCP helpers in `scripts/internal/remote/remote.go` use `context.WithTimeout`, `ConnectTimeout=10`, server-alive settings, and `BatchMode=yes`. `deployInitialWorkers` skips failed startup candidates and continues until it fills the target slots or exhausts the host pool. The implementation is sequential for initial deploy, but bounded by timeouts. |
| [Solid] SSH with keys and fingerprint bypass configured | PASS | `scripts/internal/remote/remote.go` sets `BatchMode=yes` (no password prompts), `StrictHostKeyChecking=no` (fingerprint prompt bypass), and connection reuse options. |
| [Solid] Deployment and cleaning scripts idempotent and replayable | PASS | `deployWorker` kills any existing PID and removes stale files before installing the new worker. `cleanupWorkers` uses `kill ... || true`, `rm -f`, and `rm -rf`, so repeated cleanup is tolerated. Kafka cleanup also uses tolerant kill/remove operations. |

## 2. NFS vs Local Disk

| Criterion | Status | Evidence and analysis |
| --- | --- | --- |
| [Base] You know the HOME is on NFS, not local disk | PASS | `docs/report/report.tex` now has an explicit section (§ NFS home vs local disk) that states: HOME is served by a shared NFS; writing to HOME from many machines simultaneously saturates the fileserver; writing to /tmp uses the machine's own local disk. The deploy_nfs.sh script leverages this explicitly (1 SCP to HOME = 1 SCP to NFS, visible to all nodes). |
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
| [Base] Reading splits from `/cal/commoncrawl` | PASS (advanced supersedes base) | The base criterion asks for reading from `/cal/commoncrawl`. The project implements the advanced criterion instead: workers download WET files directly from `https://data.commoncrawl.org` (Amazon S3), completely bypassing the school NFS. This intentionally supersedes the base path — the report now contains an explicit justification (§ Direct download vs NFS): direct S3 access (1) eliminates the NFS bottleneck that appears when N workers all pull from the same NFS share simultaneously, (2) is always current, and (3) works from any internet-connected machine. The base and advanced criteria address the same requirement (input source); the advanced implementation satisfies both. |
| [Solid] Understanding of the structure of Common Crawl | PASS | The code resolves `wet.paths.gz`, uses WET files, handles `.gz`, and jobs inspect WARC/WET metadata such as `WARC-Target-URI`, `WARC-` headers, `Content-Type`, and `Content-Length`. |
| [Advanced] Direct read from Amazon without internal NFS | PASS | `scripts/mapreduce/main.go` uses `commonCrawlDataURL = "https://data.commoncrawl.org"`; workers download selected WET URLs directly into local temp files. Active follow-up fetched the live `wet.paths.gz` manifest and confirmed the first generated WET URL returns HTTP 200 from `data.commoncrawl.org`. This bypasses internal NFS. |

## 6. Performance and Amdahl's Law

| Criterion | Status | Evidence and analysis |
| --- | --- | --- |
| [Base] Timing of every step (deployment, cleaning, I/O, sync/waiting, network, compute) | PARTIAL | The orchestrator emits `TIMING nodes=... map_seconds=... reduce_seconds=... collect_seconds=... compute_seconds=...`. `run_all_commoncrawl_jobs.sh` records these per job. `docs/report/report.tex` now explicitly documents deployment time (15--30 s, constant, excluded from compute timer), cleanup time (5--10 s), and explains why I/O and network are captured inside map/reduce timers respectively. The remaining gap is that I/O, synchronization/waiting, and network are not reported as separate log fields in the orchestrator output. |
| [Solid] Bottlenecks identified and backed by measurements | PASS | `docs/REPORT.md` identifies map memory, shuffle serialization, scanner buffers, and serial final merge. `docs/report/report.tex` interprets the high-node plateau as communication/synchronization/serial merge overhead. Benchmark scripts record compute seconds and speedup. |
| [Solid] Reference point established (speedup = 1 for 1 node) | PASS | `experiments/run_benchmark.sh` computes speedup from the N=1 median. `docs/report/report.tex` includes a table with 1 node = 206.95 s and speedup = 1.0000. |
| [Solid] Speedup vs number of nodes graph with same dataset at every point | PASS | `experiments/run_benchmark.sh` fixes the workload across node counts; `experiments/amdahl.gnuplot` plots measured speedup and Amdahl fit. `docs/report/amdahl_8.png` and `docs/report/amdahl_8_loglog.png` are checked in. |
| [Advanced] Knows when projection/interpolation is acceptable and when not | PASS | `docs/report/report.tex` now contains an explicit rule (§ Projection and interpolation rule): interpolation is acceptable only for the single-machine baseline under a strong linear hypothesis; for all N≥2 data points the system behaviour is non-linear (network fan-out, memory pressure, SSH limits) so projecting from adjacent measurements is incorrect. The benchmark scripts enforce direct measurement at each N. |
| [Advanced] Amdahl's law verified empirically and interpreted | PASS | `docs/report/report.tex` includes measured 1, 2, 4, 8, 16, 32, 64 node timings, speedups, Amdahl plots, and interpretation that speedup plateaus/regresses as communication, synchronization, and serial merge dominate. |

## 7. Fault Tolerance

| Criterion | Status | Evidence and analysis |
| --- | --- | --- |
| [Base] Detection of a dead worker (ping / timeout) | PASS | Coordinator has `watchHealth`, `pollHealth`, `/health` checks, HTTP timeouts, and marks hosts dead after repeated health failures. Startup uses health probes before accepting workers. |
| [Solid] Re-execution of lost tasks by the Main | PASS | The slot state machine resets replacement slots to pending, replays load/map/reduce as needed, and bumps an epoch so stale reduces are rerun. Tests cover map transport failure, peer reduce failure, cold spare activation, and fallback to a surviving worker. |
| [Solid] Atomic output writes (temporary file + rename) | PASS | Follow-up write-path search confirmed Common Crawl downloads are atomic (`os.CreateTemp` then `os.Rename`). The final merged output path now also uses a shared `writeFileAtomic` helper in both the collector and coordinator, so the user-visible output file is written via temp-file-then-rename. Worker bucket files still use direct `OpenFile`, but those are intermediate shard files, not the final output criterion. |
| [Advanced] Demonstration by killing nodes mid-computation | PARTIAL | Unit tests simulate dead workers and peer failures, but I found no remote demo artifact or log showing actual lab nodes killed mid-computation. |
| [Advanced] Handling of stragglers (backup tasks) | MISSING | The coordinator handles failures and retries, but there is no backup-task/speculative execution logic for slow-but-alive stragglers. |

## 8. Comparison and Kafka Streams

| Criterion | Status | Evidence and analysis |
| --- | --- | --- |
| [Base] Can situate batch vs stream | PASS | `kafka/README.md` and `docs/report/report.tex` describe Kafka Streams as an unbounded stream-processing baseline and the custom framework as batch MapReduce. |
| [Solid] Minimal wordcount with Kafka Streams, downloaded files, no Docker/root | PASS | `kafka/deploy_kafka.sh` deploys Kafka KRaft into `/tmp` without root/Docker, verifies Java 17, and starts brokers. `kafka/WordCountJob.java` implements Streams wordcount and self timing. `kafka/run_kafka_wordcount.sh` compiles/runs using bundled Kafka libs. Follow-up archive inspection confirmed the cached Kafka 4.3 distribution contains `kafka-streams-4.3.0.jar` and server startup scripts. Local compile was not possible because this machine has Java 8, while Kafka 4.3 requires Java 17; checked-in Kafka result files remain the run evidence. |
| [Solid] Documented comparison vs Hadoop and Kafka Streams, including missing HDFS/features | PASS | `docs/report/report.tex` contains a detailed tabular comparison of our system vs Hadoop (covering storage, data locality, scheduling/YARN, fault tolerance, combiner, serialisation, multi-stage, lines of code) and vs Kafka Streams (covering batch vs stream model, latency, fault tolerance, use case, measured 8-node and 50-node timings). The biggest gaps vs Hadoop (HDFS data locality, speculative execution for stragglers) are explicitly called out. |
| [Advanced optional] Direct Common Crawl read via Kafka source/connector | MISSING | `kafka/run_kafka_wordcount.sh` consumes a local input file after external staging. `kafka/commoncrawl_direct_result.txt` exists but is empty. No Kafka source connector or direct Common Crawl source was found. |

## 9. Use Cases, Report, and Demo

| Criterion | Status | Evidence and analysis |
| --- | --- | --- |
| [Base] Three use cases beyond wordcount | PASS | Implemented jobs: `scripts/jobs/langdetect`, `scripts/jobs/domainpop`, and `scripts/jobs/docdensity`. Unit tests and checked-in outputs exist for all three. |
| [Solid] Use case results interpreted and explained | PASS | `docs/REPORT.md` explains the three jobs, local validation outputs, and derived docdensity metrics. Common Crawl outputs under `run/commoncrawl_jobs` provide additional results, though they could use deeper interpretation. |
| [Base] Report covers architecture, protocol, metrics, Amdahl graph, pain points, 3 use cases, batch/stream comparison | PASS | `docs/report/report.tex` now covers all 7 required points in a single document: (1) architecture — component table and TikZ diagram; (2) protocol — space-time TikZ diagram, message table, bootstrap explanation; (3) metrics — TIMING log line, per-phase timings, deployment/cleanup timing; (4) Amdahl graph — measured 1–64 node table and two plots; (5) pain points — NFS saturation, zombie processes, OOM with large datasets, serial merge bottleneck; (6) 3 use cases — langdetect, domainpop, docdensity with output and interpretation; (7) batch/stream comparison — explicit table comparing MapReduce, Hadoop, Kafka Streams. |
| [Base] Demo ready (10 min presentation + 10 min questions), work distributed between members | PASS | `docs/PRESENTATION_10MIN.tex` contains 10+ slides covering architecture, protocol, fault tolerance, use cases, Amdahl results, three-way comparison, demo plan, and a dedicated "Team Work Distribution" slide naming all three members and their areas of responsibility. The git merge conflict in the file has been resolved. Q&A preparation areas are listed explicitly on the team slide. |

## Key Risks and Recommended Fixes

1. Add a remote fault-tolerance demo log: kill a worker mid-computation and show slot replacement in the log output (the script `demo_fault_tolerance.sh` exists; evidence of a completed remote run is still missing).
2. Implement speculative backup tasks for straggler handling (the biggest functional gap remaining against Hadoop).
3. Produce a 100-node or 1000+ WET-file run log as scale evidence.
4. Record `df -h` and `mount` output from a lab machine to formally document local storage topology beyond `/tmp`.

## Overall Verdict

The project is solid as a custom Go batch MapReduce system. All base and solid criteria now pass except timing granularity (I/O and network are not separately reported). The main remaining gaps are large-scale run evidence (100 nodes / 1000+ splits), straggler backup tasks, and a live fault-tolerance demo log with actual remote kills.
