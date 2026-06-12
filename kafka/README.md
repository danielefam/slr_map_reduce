# Kafka Streams Comparison Setup

Benchmarks Apache Kafka Streams against the custom Go MapReduce framework on
the same lab machines — **no root, no Docker**. Everything runs from the user
account; Kafka lives on node-local `/tmp` (never NFS, which would corrupt
Kafka's log segments and skew measurements).

## Files

| File                      | Purpose                                                            |
| ------------------------- | ------------------------------------------------------------------ |
| `deploy_kafka.sh`         | Download Kafka (KRaft mode, no ZooKeeper), install + start on N nodes from `hosts.txt` |
| `clean_kafka.sh`          | Stop brokers and remove every Kafka file from the nodes            |
| `WordCountJob.java`       | Kafka Streams word-count with built-in compute-time self-reporting |
| `run_kafka_wordcount.sh`  | End-to-end benchmark driver: topics → compile → produce → run → collect |

## Prerequisites

- `../hosts.txt` populated (run `make_hosts`, as for the MapReduce pipeline)
- SSH key access to the lab machines
- Java **17+** on the remote machines (`deploy_kafka.sh` verifies this;
  Kafka 4.x requires it). Kafka itself needs Java anyway, and
  `WordCountJob.java` is compiled remotely against the jars **bundled** in
  the Kafka distribution (`libs/*`) — no Maven/Gradle anywhere.

## Usage

```bash
cd kafka
./deploy_kafka.sh -n 3                       # 1 controller+broker, 2 brokers
./run_kafka_wordcount.sh -input ../test_input.txt -output kafka_result.txt
./clean_kafka.sh                             # always clean up when done
```

Common Crawl runs: download the same WET split the Go framework used,
decompress, and feed it as `-input`:

```bash
curl -sL https://data.commoncrawl.org/<WET-PATH> | gunzip > /tmp/cc_split.txt
./run_kafka_wordcount.sh -input /tmp/cc_split.txt
```

## How compute time is measured

Both systems report a directly comparable, machine-parseable line:

| System            | Where                              | What is timed                                                       |
| ----------------- | ---------------------------------- | -------------------------------------------------------------------- |
| Go MapReduce      | orchestrator log: `TIMING nodes=… compute_seconds=…` | map + reduce + collect (input upload `/data` is **excluded**)  |
| Kafka Streams     | `run_kafka_wordcount.sh` output: `TIMING compute_seconds=…` | Streams processing from `streams.start()` until consumer-group lag on `bench-input` reaches 0 (input production is **excluded**, done before the job starts) |

The Streams job (`WordCountJob.java`) self-terminates: a watchdog thread
polls the app's consumer-group lag via the Admin API and prints the TIMING
line when the lag is 0 twice in a row (a stable "all input processed" signal
for a bounded dataset on an unbounded streaming engine).

### Fair-comparison checklist

- Use the **same input file** and the **same number of nodes** (`-n`).
- `bench-input` is created with one partition per node, so Streams gets the
  same parallelism the MapReduce slots get.
- Run each measurement ≥ 3 times; report median (lab machines are shared).
- Note the architectural asymmetry in the report: Kafka persists every
  record to its replicated log (durability the Go framework doesn't offer),
  so Kafka pays an I/O cost per record while the Go system pays per-phase
  HTTP costs. This is a key discussion point, not a flaw in either system.
