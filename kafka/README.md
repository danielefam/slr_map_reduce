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
| `deploy_streams.sh`       | Local Kafka 4.3 quickstart deploy: downloaded files, KRaft standalone, no Docker/root |
| `run_streams_wordcount.sh` | Local bounded-file test for the bundled Kafka Streams WordCountDemo |
| `produce_commoncrawl_wet.sh` | Stream a compressed Common Crawl WET URL directly into a Kafka topic |
| `clean_streams.sh`        | Stop local WordCountDemo + broker and remove `/tmp/kafka-streams`  |

## Prerequisites

- `../hosts.txt` populated (run `make_hosts`, as for the MapReduce pipeline)
- SSH key access to the lab machines
- Java **17+** on the remote machines (`deploy_kafka.sh` verifies this;
  Kafka 4.x requires it). Kafka itself needs Java anyway, and
  `WordCountJob.java` is compiled remotely against the jars **bundled** in
  the Kafka distribution (`libs/*`) — no Maven/Gradle anywhere.

## Usage

### Local quickstart mode

This is the lightweight Kafka Streams demo path. It follows the official Kafka
4.3 quickstart using downloaded files, starts a single local KRaft broker, and
runs the bundled `org.apache.kafka.streams.examples.wordcount.WordCountDemo`.

```bash
cd kafka
./deploy_streams.sh
./run_streams_wordcount.sh -input ../test_input.txt -output streams_result.txt
./clean_streams.sh
```

For a live stream demo, keep `deploy_streams.sh` running and use Kafka's console
tools directly:

```bash
# Terminal 1: watch incremental word-count updates
/tmp/kafka-streams/kafka_2.13-4.3.0/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic streams-wordcount-output --from-beginning \
  --formatter-property print.key=true \
  --formatter-property print.value=true \
  --formatter-property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --formatter-property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer

# Terminal 2: type lines; each Enter sends one Kafka record
/tmp/kafka-streams/kafka_2.13-4.3.0/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic streams-plaintext-input
```

### Multi-node benchmark mode

```bash
cd kafka
./deploy_kafka.sh -n 3                       # 1 controller+broker, 2 brokers
./run_kafka_wordcount.sh -input ../test_input.txt -output kafka_result.txt
./clean_kafka.sh                             # always clean up when done
```

On shared lab machines, default Kafka ports may already be used by another
process. Pick high free ports when scaling to many nodes; the deploy script
writes them to `kafka_cluster.env`, and the runner reads that file automatically:

```bash
KAFKA_REMOTE_DOWNLOAD=1 KAFKA_DEPLOY_PARALLELISM=10 \
  ./deploy_kafka.sh -n 50 -hosts /tmp/kafka_hosts_50_reachable.txt \
  -port 19092 -controller-port 19093
./run_kafka_wordcount.sh -input /tmp/cc_full.wet -output kafka_full_50nodes_result.txt
```

`KAFKA_REMOTE_DOWNLOAD=1` lets each lab host download the Kafka archive directly
instead of copying the 130 MB archive from the laptop to every node. This is much
faster for large clusters and avoids local SSH/SCP fan-out bottlenecks.

Common Crawl runs: download the same WET split the Go framework used,
decompress, and feed it as `-input`:

```bash
curl -sL https://data.commoncrawl.org/<WET-PATH> | gunzip > /tmp/cc_split.txt
./run_kafka_wordcount.sh -input /tmp/cc_split.txt
```

To avoid writing the decompressed Common Crawl split to shared NFS, stream a WET
URL directly into Kafka instead:

```bash
./deploy_streams.sh
./produce_commoncrawl_wet.sh -url https://data.commoncrawl.org/<WET-PATH>.wet.gz -max-lines 10000
```

`-max-lines` is optional but useful for a quick live test. Without it, the full
compressed WET file is decompressed and sent to the input topic.

## How compute time is measured

Both systems report a directly comparable, machine-parseable line:

| System            | Where                              | What is timed                                                       |
| ----------------- | ---------------------------------- | -------------------------------------------------------------------- |
| Go MapReduce      | orchestrator log: `TIMING nodes=… compute_seconds=…` | map + reduce + collect (input upload `/data` is **excluded**)  |
| Kafka Streams     | `run_kafka_wordcount.sh` output: `TIMING compute_seconds=…` | Streams processing from `streams.start()` until consumer-group lag reaches 0 on all non-internal topics assigned to the app, including the input topic and the Streams repartition topic (input production is **excluded**, done before the job starts) |

The Streams job (`WordCountJob.java`) self-terminates: a watchdog thread
polls the app's consumer-group lag via the Admin API and prints the TIMING
line when total lag is 0 twice in a row (a stable "all input processed" signal
for a bounded dataset on an unbounded streaming engine). The runner creates a
unique input topic, output topic, and application id for each benchmark run so
old Streams internal topics cannot poison a later run with a different
partition count.

### Fair-comparison checklist

- Use the **same input file** and the **same number of nodes** (`-n`).
- The input and output topics are created with one partition per node, and
  `KAFKA_STREAM_THREADS` defaults to that same value, so one JVM can consume all
  partitions concurrently. Override it when you want a different number of
  local Streams threads.
- Run each measurement ≥ 3 times; report median (lab machines are shared).
- Note the architectural asymmetry in the report: Kafka persists every
  record to its replicated log (durability the Go framework doesn't offer),
  so Kafka pays an I/O cost per record while the Go system pays per-phase
  HTTP costs. This is a key discussion point, not a flaw in either system.

## Batch vs stream

The custom MapReduce framework is batch-oriented: a bounded input file is split,
workers map chunks, reducers aggregate intermediate keys, and the job finishes
with a final result file. Kafka Streams is stream-oriented: records arrive in a
topic over time, the WordCount topology updates a persistent local state store,
and each changed word count is emitted to an output topic immediately.

For benchmark comparison we feed the whole input file and then collect the final
latest value per word, which makes Kafka behave like a bounded batch workload.
For a streaming demonstration, type or send lines while WordCountDemo is already
running; the output topic will show count updates such as `fox -> 1`, then
`fox -> 2`, without rerunning a whole job.

## Hadoop / Kafka comparison notes

- Hadoop MapReduce normally uses HDFS for distributed, replicated input and
  output storage. This project intentionally does not implement HDFS; it uses
  local files, HTTP transfers, and worker-local temporary files.
- Hadoop provides mature scheduling, data locality, retry semantics, speculative
  execution, and fault-tolerant distributed storage. The custom framework is a
  simplified educational MapReduce runtime and does not aim to match those
  platform features.
- Kafka Streams is not Hadoop-style batch MapReduce. It is a Java stream
  processing library where Kafka topics are the input/output log and RocksDB
  state stores hold incremental aggregation state.
- Kafka is a good contrast point because it processes new records continuously;
  MapReduce is a good fit when the input is finite and the desired output is a
  final batch result.

## Optional Common Crawl source idea

The advanced extension would avoid staging large files through shared NFS by
feeding Common Crawl records directly into Kafka. This repository includes a
lightweight standalone source producer, `produce_commoncrawl_wet.sh`, that does:

```bash
curl <Common-Crawl-WET.gz> | gunzip | kafka-console-producer
```

That is not a full Kafka Connect connector, but it demonstrates the same data
path: Common Crawl HTTP source to Kafka topic without an intermediate NFS file.
A production version would package the same idea as a Kafka Connect SourceTask
with offset tracking, retries, and per-record parsing.
