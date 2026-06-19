# MapReduce Job Model

This project currently ships with multiple built-in jobs, selected via the
required `-job` flag. The pipeline itself is more general than any one job:
deployment, health checks, input distribution, map, shuffle, reduce, result
collection, retries, and cleanup are all driven by the orchestrator and worker
HTTP API. The job-specific part is the pair of functions provided by the job
package.

## Current Contract

Jobs use the types in `scripts/worker/mapreduce.go`:

```go
type KeyValue struct {
    Key   string `json:"key"`
    Value string `json:"value"`
}

type MapFunc func(docID, text string) []KeyValue
type ReduceFunc func(key string, values []string) string
```

The worker applies `MapFunc` line by line. It partitions intermediate keys with:

```text
FNV-32a(key) % N
```

where `N` is the active worker count for the job. During reduce, worker `i`
fetches bucket `i` from every peer, then applies `ReduceFunc` to each key and
its full value list.

The orchestrator's final merge parses output values as integers and sums values
for identical keys. That means reducer values must be integer strings. For
metrics such as averages, percentages, or ratios, emit summable counters and
compute the derived metric after the output file is produced.

## Built-In Job: Word Count

Input:

```text
Hello world hello
```

Map output:

```text
hello 1
world 1
hello 1
```

Reduce output:

```text
hello 2
world 1
```

This is implemented by the job package at `scripts/jobs/wordcount`. It
tokenizes Unicode letters and numbers, lowercases tokens, emits `"1"` for
every token, and reduces by counting values.

## Example Non-Word-Count Jobs

These examples fit the current integer-valued output contract. They are useful
jobs because they reuse the existing Common Crawl input path and shuffle
mechanics without changing the protocol.

### Domain Popularity

Goal: count how many records mention each web domain.

Map idea:

1. Parse each input line or WET record for URLs.
2. Normalize hostnames to lowercase.
3. Strip leading `www.` if desired.
4. Emit `(domain, "1")` for each observed domain.

Reduce idea:

```text
domain -> count(values)
```

Output example:

```text
example.org	1842
wikipedia.org	991
telecom-paris.fr	87
```

### Language Signal Count

Goal: estimate language distribution using simple stop-word signals.

Map idea:

1. Lowercase each line.
2. Score it against small stop-word sets such as English, French, Italian, and
   Spanish.
3. Emit `(lang:<code>, "1")` for the best-scoring language when confidence is
   above a threshold.

Reduce idea:

```text
lang:<code> -> count(values)
```

Output example:

```text
lang:en	12034
lang:fr	3918
lang:it	640
```

### Document Density Histogram

Goal: classify documents by approximate text length or token density.

Map idea:

1. Count characters or tokens in each document/line.
2. Choose a bucket such as `bytes:0-1k`, `bytes:1k-10k`, `bytes:10k-100k`.
3. Emit `(bucket, "1")`.

Reduce idea:

```text
bucket -> count(values)
```

Output example:

```text
bytes:0-1k	812
bytes:1k-10k	4301
bytes:10k-100k	276
```

## How To Add A Job Today

The CLI already accepts `-job`. To add a new job today:

1. Add a new package under `scripts/jobs/<name>/` exporting `NewMapper()` and
   `NewReducer()`.
2. Keep reducer outputs integer-valued so the orchestrator can merge them.
3. Run the normal MapReduce command with the new job path:

```bash
./mapreduce.sh -job scripts/jobs/<name> -input test_input.txt -output result.txt -n 10 -port 9090
```

## Recommended Future CLI

A clean next step is to add a short-name registry for the existing `-job` flag:

```bash
./mapreduce.sh -job wordcount -input test_input.txt -output result.txt -n 10
./mapreduce.sh -job domainpop -commoncrawl -crawl CC-MAIN-2026-05 -files-limit 20 -output domains.txt -n 20
```

The registry should keep job selection local to the worker build and preserve
the existing HTTP protocol. The orchestrator would pass the job name at build
time or generate a small binding file before compiling the worker.