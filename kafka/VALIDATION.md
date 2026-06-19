# Kafka Streams Validation

Date: 2026-06-19

This note records how the Kafka part satisfies the four requested objectives and the concrete tests used to validate it.

## Requirement Coverage

| Level | Requirement | Status | Evidence |
| --- | --- | --- | --- |
| Base | Situate batch vs stream | Working | Documented in `README.md` under "Batch vs stream". |
| Solid | Minimal wordcount with Kafka Streams, downloaded files, no Docker/root | Working | `deploy_streams.sh`, `run_streams_wordcount.sh`, and `clean_streams.sh` run Kafka 4.3 from the downloaded `kafka_2.13-4.3.0.tgz` distribution. The local setup uses KRaft standalone mode and the bundled `WordCountDemo`. |
| Solid | Documented comparison vs Hadoop & Kafka Streams | Working | Documented in `README.md` under "Hadoop / Kafka comparison notes", including missing HDFS and other platform features. |
| Advanced optional | Direct Common Crawl read via Kafka source / connector path | Working as lightweight source producer | `produce_commoncrawl_wet.sh` streams `curl <WET.gz> | gunzip | kafka-console-producer`, avoiding a decompressed file on shared NFS. |

## Files Added or Used

| File | Purpose |
| --- | --- |
| `deploy_streams.sh` | Starts local Kafka 4.3 from downloaded files, no Docker/root. |
| `run_streams_wordcount.sh` | Runs a bounded-file wordcount test and writes final counts to `streams_result.txt`. |
| `clean_streams.sh` | Stops WordCountDemo and Kafka broker, then removes `/tmp/kafka-streams`. |
| `produce_commoncrawl_wet.sh` | Streams a compressed Common Crawl WET URL directly into the Kafka input topic. |
| `README.md` | Documents usage, batch vs stream, Hadoop comparison, and Common Crawl source idea. |
| `deploy_kafka.sh` / `run_kafka_wordcount.sh` / `clean_kafka.sh` | Deploy, run, and clean the multi-node Kafka Streams benchmark. |
| `WordCountJob.java` | Custom Kafka Streams wordcount with timing, all-topic lag detection, unique run topics, and final state-store result export. |

## Test 1: Minimal Kafka Streams WordCount

Command sequence:

```bash
cd kafka
./deploy_streams.sh
./run_streams_wordcount.sh -input ../test_input.txt -output streams_result.txt
```

Observed result:

```text
Results: streams_result.txt (46 distinct words)
TIMING compute_seconds=10,156
```

Sample final output from `streams_result.txt`:

```text
the     7
are     5
and     4
animals 4
brown   4
fox     4
foxes   4
lazy    4
quick   4
```

This proves the minimal Kafka Streams wordcount path works and produces the same style of final word-count output as the custom MapReduce framework.

## Test 2: Live Streaming Behavior

A live consumer was started on the output topic:

```bash
/tmp/kafka-streams/kafka_2.13-4.3.0/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic streams-wordcount-output --from-beginning \
  --formatter-property print.key=true \
  --formatter-property print.value=true \
  --formatter-property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --formatter-property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
```

A live producer sent these messages one at a time:

```text
the quick brown fox
the fox jumps again
quick streams count fox
```

Observed output topic updates:

```text
the     1
quick   1
brown   1
fox     1
the     2
fox     2
jumps   1
again   1
quick   2
streams 1
count   1
fox     3
```

This proves the stream behavior: Kafka Streams does not wait for a whole batch. It emits updated counts as new records arrive.

## Test 3: Direct Common Crawl Source Producer

A real Common Crawl WET path was fetched from:

```bash
curl -fsL https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-21/wet.paths.gz | gunzip -c | head -1
```

Resolved WET URL:

```text
https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-21/segments/1778213376756.47/wet/CC-MAIN-20260508074046-20260508104046-00000.warc.wet.gz
```

Direct producer command:

```bash
./produce_commoncrawl_wet.sh \
  -url https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-21/segments/1778213376756.47/wet/CC-MAIN-20260508074046-20260508104046-00000.warc.wet.gz \
  -max-lines 5
```

Observed input topic messages:

```text
WARC/1.0
WARC-Type: warcinfo
WARC-Date: 2026-05-21T23:30:56Z
WARC-Filename: CC-MAIN-20260508074046-20260508104046-00000.warc.wet.gz
WARC-Record-ID: <urn:uuid:c3b7298d-9518-465e-b581-1b58ca19ede0>
```

Observed Kafka Streams output updates:

```text
warc    1
1       1
0       1
warc    2
type    1
warcinfo        1
warc    3
date    1
2026    1
05      1
```

This proves the advanced path works as a lightweight source producer:

```text
Common Crawl HTTP .wet.gz -> gunzip stream -> Kafka input topic -> Kafka Streams WordCount output
```

It avoids creating a large decompressed Common Crawl file on shared NFS.

No-limit validation was also run against the same WET file without `-max-lines`.
The input topic reached 4,023,713 records, proving the direct source path can
stream the full compressed Common Crawl split into Kafka without staging the
decompressed file on shared NFS.

## Test 4: 8-node Full Common Crawl Kafka Streams Benchmark

Remote cluster deployment used 8 lab machines from `hosts_go_pool_300_filtered.txt`:

```text
tp-1a201-01.enst.fr ... tp-1a201-08.enst.fr
```

The deployment script verified Java 17 on each machine and started all 8 Kafka
KRaft brokers. Kafka was installed under user-specific node-local `/tmp`:

```text
/tmp/kafka-bench-$USER
```

The full WET input was prepared on local `/tmp`, not shared NFS:

```bash
curl -fsL https://data.commoncrawl.org/crawl-data/CC-MAIN-2026-21/segments/1778213376756.47/wet/CC-MAIN-20260508074046-20260508104046-00000.warc.wet.gz \
  | gunzip -c > /tmp/cc_full.wet
wc -l /tmp/cc_full.wet
du -h /tmp/cc_full.wet
```

Observed input size:

```text
4023713 /tmp/cc_full.wet
167M    /tmp/cc_full.wet
```

Final benchmark command:

```bash
./run_kafka_wordcount.sh -input /tmp/cc_full.wet -output kafka_full_8nodes_result.txt
```

Observed final run:

```text
Run ID: 20260619115916
Stream threads: 8
Results: kafka_full_8nodes_result.txt (1738969 distinct words)
TIMING compute_seconds=36,253
```

Result file evidence:

```text
-rw-rw-r-- 1 daniele daniele 30M juin  19 12:00 kafka_full_8nodes_result.txt
1738969 kafka_full_8nodes_result.txt
the     239298
to      189824
de      180013
and     178385
a       173826
warc    169316
```

The large test exposed and fixed several issues that only appear under repeated
multi-node runs:

- Remote Kafka files now use `/tmp/kafka-bench-$USER`, avoiding permission
  collisions with old `/tmp/kafka-bench` directories from other users/runs.
- The runner uses a unique input topic, output topic, and Kafka Streams
  application id per run, avoiding stale internal repartition topics with the
  wrong partition count.
- `WordCountJob` waits for total lag across all assigned non-internal topics,
  including the Streams repartition topic, not just the original input topic.
- `KAFKA_STREAM_THREADS` defaults to the partition count, so the 8-partition
  benchmark uses 8 local stream threads instead of bottlenecking on one thread.
- Final counts are exported from the Kafka Streams state store as `RESULT`
  lines instead of consuming every intermediate update from the output topic.

## Test 5: 50-node Full Common Crawl Kafka Streams Benchmark

The same full WET input was used for a larger Kafka test on 50 reachable lab
machines selected from `hosts_go_pool_300_filtered.txt`:

```text
50 kafka_hosts.txt
4023713 /tmp/cc_full.wet
167M    /tmp/cc_full.wet
```

The first 50-node attempt exposed a shared-lab port collision: the default
KRaft controller port `9093` was already bound on the selected controller host,
causing follower brokers to report `INCONSISTENT_CLUSTER_ID`. The deploy script
was updated to support a separate `-controller-port`, to persist cluster settings
in `kafka_cluster.env`, and to wait for the controller before starting followers.
The successful run used high free ports on all 50 nodes:

```text
BROKER_PORT=19092
CONTROLLER_PORT=19093
KAFKA_VERSION=4.3.0
SCALA_VERSION=2.13
REMOTE_ROOT=/tmp/kafka-bench-daniele
```

Successful deployment evidence:

```text
Kafka cluster ready. Bootstrap server: tp-1a201-02.enst.fr:19092
50 brokers accepted connections on port 19092
```

Final benchmark command:

```bash
./run_kafka_wordcount.sh -input /tmp/cc_full.wet -output kafka_full_50nodes_result.txt
```

Observed final run:

```text
Run ID: 20260619215336
Stream threads: 50
Input topic offsets: 50 partitions, 4,023,713 records
Results: kafka_full_50nodes_result.txt (1738969 distinct words)
TIMING compute_seconds=47,363
```

Result file evidence:

```text
-rw-rw-r-- 1 daniele daniele 30M juin  19 21:56 kafka_full_50nodes_result.txt
1738969 kafka_full_50nodes_result.txt
the     239298
to      189824
de      180013
and     178385
a       173826
warc    169316
```

This run validates the new Kafka part at 50 nodes. It also confirms that the
final counts match the 8-node run for the same input, while stressing deployment
and startup behavior at a much larger node count.

## Hadoop / Kafka Comparison Summary

The custom framework is closer to educational batch MapReduce: it splits a finite input, sends work to workers, aggregates intermediate key/value pairs, and writes a final result file.

Hadoop MapReduce adds major production features that this project intentionally does not implement:

- HDFS for distributed replicated storage.
- Data locality scheduling.
- Fault-tolerant task retries.
- Speculative execution.
- Mature cluster resource management.
- Large ecosystem integration.

Kafka Streams is different again: Kafka topics are the input/output logs, and the application maintains incremental state, commonly in RocksDB-backed state stores. It is better suited for continuously arriving records, while MapReduce is naturally suited to finite batch jobs.

## Cleanup

After validation, Kafka was stopped with:

```bash
./clean_streams.sh
./clean_kafka.sh
```

Observed cleanup:

```text
=== Cleaning Kafka Streams local setup ===
Done. To redeploy: ./deploy_streams.sh
=== Cleaning Kafka from 8 node(s) ===
--- tp-1a201-01.enst.fr ---
  ✓ cleaned
...
--- tp-1a201-08.enst.fr ---
  ✓ cleaned
Done.
=== Cleaning Kafka from 50 node(s) ===
--- tp-1a201-02.enst.fr ---
  ✓ cleaned
...
--- tp-1a252-14.enst.fr ---
  ✓ cleaned
Done.
```

The local Kafka setup was clean, and the 8-node and 50-node remote Kafka
clusters were removed from `/tmp/kafka-bench-$USER` on all benchmark hosts.
During cleanup validation, `clean_kafka.sh` was hardened so `pkill -f` patterns
cannot match and kill their own SSH shell.
