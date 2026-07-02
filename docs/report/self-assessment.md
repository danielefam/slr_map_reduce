# Self-Assessment Questionnaire

Checklist for the distributed MapReduce final project. This is filled honestly from the current repository state and the fresh verification work performed on 2026-07-01.

## 1. Infrastructure and deployment

- [x] List of 100 live machines generated automatically using `ajax.php`, with canonical `.enst.fr` names.
- [x] Deployment script = 1 SCP to the HOME (NFS) + 100 SSH to start the servers.
- [x] Servers listen on a chosen port, with no collision with an already-used port.
- [x] Cleaning script that kills all deployed servers; clean redeployment possible.
- [x] Deployment robust to unreachable machines (timeout, no blocking).
- [x] SSH with keys + fingerprint bypass configured.
- [x] Deployment and cleaning scripts idempotent and replayable.

## 2. NFS vs local disk

- [x] HOME is on NFS, not local disk.
- [x] Map intermediate files are written to local disk (`/tmp`), not the NFS.
- [x] NFS load is deliberately limited by using local temp storage and direct peer-to-peer shuffle.
- [ ] Explored storage beyond `/tmp` using `df` / `mount` and used it appropriately.

## 3. Protocol design

- [x] Main / Workers roles clearly defined.
- [x] Space-time diagram of the V1 protocol done in sequencediagram.org.
- [x] Synchronization of the map → shuffle → reduce phases working.
- [x] System bootstrap handled cleanly.
- [x] Protocol formalized and documented (messages, formats, edge cases).

## 4. MapReduce core

- [x] Word frequency works on small splits.
- [x] Key distribution at reduce via hash(key) mod N.
- [x] Remote read of local intermediate files for the reduce phase.
- [x] Results validated against a single-machine reference computation on the same small dataset.
- [ ] Works on real Common Crawl splits at scale (100, 1000+).

## 5. Common Crawl data

- [ ] Reading splits from `/cal/commoncrawl`.
- [x] Understanding of the structure of Common Crawl.
- [x] Direct read from Amazon without going through the internal NFS.

## 6. Performance and Amdahl's law

- [x] Timing of every step (deployment, cleaning, I/O, sync/waiting, network, compute).
- [x] Bottlenecks identified and backed by measurements.
- [x] Reference point established (speedup = 1 for 1 node).
- [x] Speedup vs number of nodes graph with the same dataset at every point.
- [x] Knows when projection/interpolation is acceptable and when it is not.
- [x] Amdahl's law verified empirically and interpreted.

## 7. Fault tolerance

- [x] Detection of a dead worker (ping / timeout).
- [x] Re-execution of lost tasks by the Main.
- [x] Atomic output writes (temporary file + rename).
- [x] Demonstration of tolerance by killing nodes mid-computation.
- [ ] Handling of stragglers (backup tasks).

## 8. Comparison and Kafka Streams

- [x] Can situate batch vs stream.
- [x] Minimal wordcount with Kafka Streams (downloaded files, no Docker or root).
- [x] Documented comparison vs Hadoop & Kafka Streams, including the missing HDFS/features.
- [ ] Direct Common Crawl read via a Kafka source / connector.

## 9. Use cases, report, and demo

- [x] 3 use cases beyond wordcount.
- [x] Use case results interpreted and explained.
- [x] Report covering architecture, protocol, metrics, Amdahl's graph, pain points, 3 use cases, and batch/stream comparison.
- [x] Demo ready, with work distributed between members.

## Wrap-up Questions

### How many Base criteria are ticked?

### How many Base criteria are ticked?

15 out of 16 base criteria are ticked in this honest repository-based self-assessment. (Reading splits from /cal/commoncrawl was skipped in favor of direct Amazon S3 download to avoid NFS saturation).

### Is the work fairly shared and is the collaboration healthy?

Yes, the presentation explicitly details the team work distribution (Guilherme Caporali: deployment/fault tolerance/Amdahl, Daniele Famà: architecture/orchestrator/protocol, Marcin Porwisz: Kafka/WordCountJob.java). The collaboration appears healthy and balanced.

### Is someone clearly owning the Kafka Streams part?

Yes, Marcin Porwisz clearly owns the Kafka Streams comparison, as detailed in the presentation.

### What are the two weakest points today?

The first weak point is the lack of 100-node / 1000+ split scale evidence. The second weak point is the lack of speculative execution for stragglers.

### What broke and why?

The main pain point was NFS saturation when all nodes read `/cal/commoncrawl` simultaneously, causing extreme slowdowns. This was fixed by transitioning to a direct Amazon S3 download which scales with node count. We also encountered out-of-memory errors when processing large datasets because intermediate data is heavily buffered in memory before being written to `/tmp`.

### If the demo were tomorrow, what would not pass?

The demo would only be weak on showing speculative straggler handling and 100+ node scale evidence, but everything else including fault tolerance, Amdahl metrics, and Kafka comparison is complete and verified.