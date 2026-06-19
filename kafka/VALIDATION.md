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
```

Observed cleanup:

```text
Stopping WordCountDemo
Stopping Kafka broker
Removing /tmp/kafka-streams
Done.
```
