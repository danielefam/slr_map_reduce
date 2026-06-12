# SLR Project P4 — Distributed MapReduce & Remote Stats Collection

This project provides two independent distributed workflows that operate over a
pool of remote lab machines (`*.enst.fr`):

| Script         | Purpose                                                                    |
| -------------- | -------------------------------------------------------------------------- |
| `mapreduce.sh` | Run a distributed **word-count MapReduce** job across N remote workers     |
| `run.sh`       | Deploy an HTTP server to remote hosts, collect system stats, then clean up |

Both workflows are written in Go and share a common `hosts.txt` discovery mechanism.

---

## Repository Layout

```
.
├── manifest.json       # Configuration for deploy/collect/cleanup tools
├── hosts.txt           # Auto-generated list of available remote hosts
├── mapreduce.sh        # Entry point for the MapReduce pipeline
├── run.sh              # Entry point for the stats-collection pipeline
└── scripts/
    ├── go.mod
    ├── make_hosts/     # Fetches available hosts from tp.telecom-paris.fr
    ├── deploy/         # Copies files to NFS and runs commands via SSH
    ├── collect/        # Collects stats from remote hosts via SSH
    ├── cleanup/        # Runs cleanup commands and removes NFS files
    ├── mapreduce/      # Client-side MapReduce orchestrator
    └── worker/         # HTTP worker server (map / reduce)
```

---

## Prerequisites

- Go 1.25+
- SSH key-based access (no password) to the remote machines
- Network access to `tp.telecom-paris.fr` (for host discovery)

---

## Quick Start

### Word-Count MapReduce

```bash
./mapreduce.sh -input /path/to/data.txt -output result.txt -n 10 -port 9090
# or, from the official Common Crawl website:
./mapreduce.sh -commoncrawl -crawl CC-MAIN-2026-05 -files-limit 4 -output result.txt -n 10 -port 9090
```

| Flag            | Default      | Description                                                                           |
| --------------- | ------------ | ------------------------------------------------------------------------------------- |
| `-input`        | _(optional)_ | Path to the input text file                                                           |
| `-commoncrawl`  | _(optional)_ | Use the official Common Crawl website instead of a local file                         |
| `-crawl`        | latest crawl | Common Crawl ID override for reproducible website-backed runs                         |
| `-files-limit`  | `0`          | Cap the number of Common Crawl WET files selected (`0` = all)                        |
| `-chunks-limit` | `0`          | Second Common Crawl workload cap kept for benchmark compatibility (`0` = disabled)   |
| `-output`       | `result.txt` | Path for the merged output file                                                       |
| `-n`            | `10`         | Number of worker nodes to use                                                         |
| `-port`         | `9090`       | HTTP port for worker servers                                                          |

The output file contains one `word<TAB>count` line per word, sorted by
descending count then alphabetically.

### Remote Stats Collection

```bash
./run.sh
```

Connects to 100 hosts, starts an HTTP server on each, collects `hostname`,
`uptime`, and memory stats, writes them to `stats.txt`, and cleans everything
up on exit.

---

## Configuration — `manifest.json`

```json
{
  "nfs": "",
  "files": [],
  "n": 100,
  "commands": [
    "nohup python3 -m http.server 8080 </dev/null >/tmp/httpserver.log 2>&1 & echo $! > /tmp/httpserver.pid"
  ],
  "collect_commands": ["hostname", "uptime", "free -m | head -2"],
  "cleanup_commands": [
    "kill $(cat /tmp/httpserver.pid) 2>/dev/null || true",
    "rm -f /tmp/httpserver.pid /tmp/httpserver.log",
    "kill $(cat /tmp/mr-worker.pid) 2>/dev/null || true",
    "rm -f /tmp/mr-worker.pid /tmp/mr-worker.log /tmp/mr-worker"
  ]
}
```

| Field              | Used by             | Description                                   |
| ------------------ | ------------------- | --------------------------------------------- |
| `nfs`              | `deploy`, `cleanup` | SCP destination for shared files              |
| `files`            | `deploy`, `cleanup` | Local files to copy to the NFS share          |
| `n`                | all                 | Max number of hosts (0 = all)                 |
| `commands`         | `deploy`            | Shell commands run on each host after deploy  |
| `collect_commands` | `collect`           | Commands whose output is saved to `stats.txt` |
| `cleanup_commands` | `cleanup`           | Commands run on each host to clean up         |

---

## Components

### `make_hosts`

Queries `https://tp.telecom-paris.fr/ajax.php` for the list of available
machines and writes the first `-n` entries to a local file.

```
make_hosts -n 10 -f hosts.txt
```

### `deploy`

Copies files to an NFS share (via `scp`) and then SSH-executes `commands`
on each host in parallel.

```
deploy -m manifest.json -h hosts.txt
```

### `collect`

SSH-runs `collect_commands` on each host in parallel and writes a
per-host report to an output file.

```
collect -m manifest.json -h hosts.txt -o stats.txt
```

### `cleanup`

SSH-runs `cleanup_commands` on each host in parallel and removes deployed
files from the NFS share.

```
cleanup -m manifest.json -h hosts.txt
```

### `worker`

An HTTP server that holds the state for one MapReduce job. Started on each
remote node by the `mapreduce` orchestrator.

| Endpoint        | Method | Description                                                                                    |
| --------------- | ------ | ---------------------------------------------------------------------------------------------- |
| `/health`       | GET    | Readiness probe — returns `200 ok` when ready                                                  |
| `/data`         | POST   | Receive a raw text chunk (the worker's input partition)                                        |
| `/load`         | POST   | Accept a JSON payload of Common Crawl WET URLs; downloads them into the worker `workDir`       |
| `/map`          | POST   | Run the map function on the local input; body is `{"id": N, "peers": [...]}`                  |
| `/intermediate` | GET    | Serve the pre-partitioned map output bucket for a reducer (`?reducer=N&n=N`) — written at map time, served in O(1) without scanning |
| `/reduce`       | POST   | Pull intermediate KVs from all peers via `/intermediate`, then run the reduce function locally |
| `/result`       | GET    | Download the reduce output as `key\tvalue` lines                                               |

### `mapreduce` (orchestrator)

Coordinates the full pipeline from the client machine:

1. Read N hosts from `hosts.txt`
2. Build the worker binary (`GOOS=linux GOARCH=amd64`)
3. SCP the binary to each node
4. SSH each node to start the worker HTTP server (`nohup`)
5. Wait for all workers to pass the health check
6. Distribute input — either split a local file (`-input`) into 64 MB chunks and push them via `POST /data`, **or** resolve Common Crawl WET URLs from the official website and assign them **round-robin** across the N workers via `POST /load`. Workers download those WET files into their local `workDir`, map from local file streams, and delete the downloaded inputs after map succeeds. Bound Common Crawl runs with `-files-limit` and/or `-chunks-limit`.
7. Broadcast `POST /map` with each worker's ID and the full peer list — each worker partitions its intermediate KVs into N bucket files (`map-bucket-{n}-{idx}.jsonl`) using `FNV-32a(key) % N`
8. Broadcast `POST /reduce` — each worker fetches its pre-partitioned bucket file from every peer via `GET /intermediate?reducer=id&n=N` (O(1) file read per peer) and runs reduce locally
9. `GET /result` from every worker, merge word counts, sort (descending count, then alphabetical), write output file
10. SSH cleanup: kill worker processes and remove temporary files

---

## Sequence Diagrams

### 1. Stats-Collection Pipeline (`run.sh`)

```mermaid
sequenceDiagram
    participant User
    participant run.sh
    participant make_hosts
    participant deploy
    participant collect
    participant cleanup
    participant Hosts as Remote Hosts (×N)

    User->>run.sh: ./run.sh
    run.sh->>make_hosts: -n 100 -f hosts.txt
    make_hosts->>tp.telecom-paris.fr: GET /ajax.php
    tp.telecom-paris.fr-->>make_hosts: available hosts JSON
    make_hosts-->>run.sh: hosts.txt written

    run.sh->>deploy: -m manifest.json -h hosts.txt
    par for each host (parallel)
        deploy->>Hosts: SSH: run commands (start HTTP server)
        Hosts-->>deploy: OK
    end
    deploy-->>run.sh: done

    run.sh->>collect: -m manifest.json -h hosts.txt -o stats.txt
    par for each host (parallel)
        collect->>Hosts: SSH: hostname && uptime && free -m
        Hosts-->>collect: output
    end
    collect-->>run.sh: stats.txt written

    run.sh->>cleanup: -m manifest.json -h hosts.txt
    par for each host (parallel)
        cleanup->>Hosts: SSH: kill HTTP server & remove files
        Hosts-->>cleanup: OK
    end
    cleanup-->>run.sh: done
    run.sh-->>User: All done. Stats are in stats.txt
```

---

### 2. MapReduce Pipeline

```mermaid
sequenceDiagram
    participant Orchestrator
    participant W0 as Worker 0
    participant W1 as Worker 1
    participant Wn as Worker N-1

    Note over Orchestrator: Split a local file into ≤64 MB chunks on line boundaries<br/>or assign Common Crawl WET URLs round-robin

    par Distribute data (parallel)
        Orchestrator->>W0: POST /data  (chunk 0)
        Orchestrator->>W1: POST /data  (chunk 1)
        Orchestrator->>Wn: POST /data  (chunk N-1)
    end
    Note over W0,Wn: In Common Crawl mode, the orchestrator sends POST /load with WET URLs<br/>and each worker downloads its assigned files into workDir before /map

    par Map phase (parallel)
        Orchestrator->>W0: POST /map  {"id":0, "peers":[...]}
        Orchestrator->>W1: POST /map  {"id":1, "peers":[...]}
        Orchestrator->>Wn: POST /map  {"id":N-1, "peers":[...]}
    end
    Note over W0,Wn: Each worker partitions intermediate KVs into N bucket files<br/>(map-bucket-{n}-{idx}.jsonl) keyed by FNV-32a(key) % N
    W0-->>Orchestrator: 200
    W1-->>Orchestrator: 200
    Wn-->>Orchestrator: 200

    par Reduce phase (parallel)
        Orchestrator->>W0: POST /reduce
        Orchestrator->>W1: POST /reduce
        Orchestrator->>Wn: POST /reduce
        Note over W0,Wn: Each worker fetches its pre-partitioned bucket from all peers<br/>via GET /intermediate?reducer=id&n=N (O(1) file read, no scan),<br/>then reduces locally
        W0->>W1: GET /intermediate?reducer=0&n=N
        W0->>Wn: GET /intermediate?reducer=0&n=N
        W1->>W0: GET /intermediate?reducer=1&n=N
        W1->>Wn: GET /intermediate?reducer=1&n=N
    end
    W0-->>Orchestrator: 200
    W1-->>Orchestrator: 200
    Wn-->>Orchestrator: 200

    par Collect results (parallel)
        Orchestrator->>W0: GET /result
        Orchestrator->>W1: GET /result
        Orchestrator->>Wn: GET /result
        W0-->>Orchestrator: key-values
        W1-->>Orchestrator: key-values
        Wn-->>Orchestrator: key-values
    end

    Note over Orchestrator: Merge counts, sort, write output file
```

---

### 3. Intermediate Data Pull Detail

This diagram zooms in on how intermediate data flows during the reduce phase for a 3-worker example.

```mermaid
sequenceDiagram
    participant W0 as Worker 0
    participant W1 as Worker 1
    participant W2 as Worker 2

    Note over W0,W2: Orchestrator triggers POST /reduce on all workers simultaneously

    par W0 pulls its reducer bucket (id=0) from all peers
        W0->>W0: GET /intermediate?reducer=0&n=3 (self)
        W0->>W1: GET /intermediate?reducer=0&n=3
        W0->>W2: GET /intermediate?reducer=0&n=3
    end

    par W1 pulls its reducer bucket (id=1) from all peers
        W1->>W0: GET /intermediate?reducer=1&n=3
        W1->>W1: GET /intermediate?reducer=1&n=3 (self)
        W1->>W2: GET /intermediate?reducer=1&n=3
    end

    par W2 pulls its reducer bucket (id=2) from all peers
        W2->>W0: GET /intermediate?reducer=2&n=3
        W2->>W1: GET /intermediate?reducer=2&n=3
        W2->>W2: GET /intermediate?reducer=2&n=3 (self)
    end

    Note over W0,W2: Each peer serves its pre-partitioned bucket file directly (written at map time).<br/>No full-scan: FNV-32a(key) % N routing was done once during POST /map.<br/>After fetching: each worker owns all values for its disjoint key set — ready to reduce
```

---

## MapReduce Data Flow

```
Input file
    │
    ▼  (split on newline boundaries, ≤64 MB each)
┌───────┬───────┬───────┐
│Chunk 0│Chunk 1│Chunk N│  ── POST /data ──▶  Workers
└───────┴───────┴───────┘
(or: .wet/.wet.gz files assigned round-robin ── POST /load ──▶ Workers)

Workers: Map phase
    Each worker applies wordCountMap line-by-line:
    "hello world hello" → [(hello,1),(world,1),(hello,1)]
    Output: N pre-partitioned bucket files on disk (map-bucket-{n}-{idx}.jsonl)
    Keys routed by FNV-32a(key) % N — partitioning done once, at map time

Workers: Reduce phase  (pull-based, no orchestrator involvement)
    Each worker i queries every peer (including itself) for its bucket:
      GET /intermediate?reducer=i&n=N
    Peers serve map-bucket-{n}-{i}.jsonl directly — O(1), no scan
    After fetching: each worker owns all values for a disjoint key set
    wordCountReduce(key, values) = len(values)
    Output: sorted []KeyValue

Orchestrator: Collect phase
    GET /result from all workers
    Merge totals (sum counts across workers for same key)
    Sort: descending count, then alphabetical
    Write to output file
```

---

## Word-Count Functions

| Function          | Signature                    | Behaviour                                                                           |
| ----------------- | ---------------------------- | ----------------------------------------------------------------------------------- |
| `wordCountMap`    | `(docID, text) → []KeyValue` | Splits text on non-alphanumeric runes; emits `(lowercase_word, "1")` for each token |
| `wordCountReduce` | `(key, []string) → string`   | Returns `len(values)` as a string (counts occurrences)                              |

The map and reduce functions are pluggable via the `MapFunc` / `ReduceFunc` type
aliases, so the worker can be adapted to other jobs.

---

## Building

Each script can be compiled individually:

```bash
# Build the worker binary for the local platform
cd scripts && go build -o /tmp/mr-worker ./worker/

# Build any other tool
cd scripts && go build -o /tmp/mapreduce ./mapreduce/
cd scripts && go build -o /tmp/deploy    ./deploy/
cd scripts && go build -o /tmp/collect   ./collect/
cd scripts && go build -o /tmp/cleanup   ./cleanup/
cd scripts && go build -o /tmp/make_hosts ./make_hosts/

# Vet everything
cd scripts && go vet ./worker/ ./mapreduce/ ./deploy/ ./collect/ ./cleanup/ ./make_hosts/
```

The `mapreduce` orchestrator cross-compiles the worker automatically
(`GOOS=linux GOARCH=amd64`) before deploying it.

---

## Benchmarking — Amdahl's Law

The `experiments/` directory contains a strong-scaling harness that runs the
**same fixed workload** while doubling the number of worker nodes
(`1, 2, 4, …, 128`), measures the **compute time** (map → reduce → collect only,
excluding build/deploy/cleanup overhead), and plots speedup against Amdahl's law.

The orchestrator emits a machine-parseable timing line at the end of each run:

```
TIMING nodes=8 map_seconds=… reduce_seconds=… collect_seconds=… compute_seconds=…
```

The fixed workload is a set of WET files split into 64 MB chunks and distributed
round-robin across the active workers, so total work stays constant as N grows.
Bound it with `-files-limit` and/or `-chunks-limit`.

### Step-by-step guide

**Prerequisites**

- The same SSH key-based access to the `*.enst.fr` hosts used by the main
  pipeline (the runner deploys workers exactly like `mapreduce.sh`).
- Network access to `tp.telecom-paris.fr` for host discovery (or a pre-built
  `experiments/bench-hosts.txt` plus the `-no-fetch` flag).
- `gnuplot` installed (only needed for `-plot` / regenerating the figure).
- Network access from the workers to `data.commoncrawl.org` and
  `index.commoncrawl.org`.

**1. Pick the fixed workload.** Choose one of:

- `-chunks-limit N` — keep a second deterministic workload cap for compatibility
  with existing benchmark flows. Use at least a few URLs per worker at the
  largest node count, e.g. `-chunks-limit 256` for a sweep that reaches 128.
- `-files-limit N` — use the first `N` WET files from the selected crawl.

**2. Run the sweep.** From the repository root:

```bash
# Recommended: sweep 1→128, 3 reps/N, and plot at the end.
experiments/run_benchmark.sh \
    -crawl CC-MAIN-2026-05 \
    -chunks-limit 256 \
    -nodes "1 2 4 8 16 32 64 128" \
    -reps 3 \
    -plot
```

What happens for each node count `N` in the sequence:

1. The runner fetches a master host pool once (`make_hosts -n <maxN>`), then
   slices the first `N` hosts for the run.
2. It invokes the MapReduce orchestrator `-reps` times. Each run loads the data
   **before** starting the timer, then times only map → reduce → collect and
   prints `TIMING nodes=N … compute_seconds=…`.
3. The runner parses `compute_seconds`, validates that the orchestrator actually
   used `N` active workers (otherwise the run is discarded), and records the
   **median** of the surviving runs.

**3. Read the generated data.** The runner writes `experiments/results.csv`:

```
nodes,median_seconds,speedup,runs
1,420.300,1.0000,418.1;420.3;422.0
2,214.500,1.9594,214.0;214.5;215.2
4,110.800,3.7934,…
…
```

| Column           | Meaning                                                        |
| ---------------- | ------------------------------------------------------------- |
| `nodes`          | Number of active worker nodes (N)                             |
| `median_seconds` | Median compute time over the `-reps` runs                     |
| `speedup`        | `median_seconds[N=1] / median_seconds[N]`                     |
| `runs`           | All successful per-run compute times (`;`-separated)          |

**4. Generate / regenerate the plot.** `-plot` renders `experiments/amdahl.png`
automatically. To (re)build it from an existing CSV:

```bash
gnuplot -e "datafile='experiments/results.csv'; outfile='experiments/amdahl.png'" \
    experiments/amdahl.gnuplot
```

The plot shows the measured speedup (points), a fitted Amdahl curve
`S(N) = 1 / ((1 - p) + p/N)` with the estimated parallel fraction `p`, and an
ideal linear-speedup reference. The x-axis uses a log-base-2 scale; the y-axis
is linear speedup.

**Re-running without re-fetching hosts.** To reuse the same machines across
sweeps (and skip host discovery), pass `-no-fetch` so the runner reuses the
existing `experiments/bench-hosts.txt`:

```bash
experiments/run_benchmark.sh -crawl CC-MAIN-2026-05 -chunks-limit 256 \
    -no-fetch -plot
```

### Outputs

| Path                          | Description                                  |
| ----------------------------- | -------------------------------------------- |
| `experiments/results.csv`     | Per-N median compute time and speedup        |
| `experiments/amdahl.png`      | Amdahl's-law plot (with `-plot`)             |
| `experiments/bench-hosts.txt` | Master host pool (regenerated unless `-no-fetch`) |

### Flags

| Flag            | Default               | Purpose                                            |
| --------------- | --------------------- | -------------------------------------------------- |
| `-crawl`        | latest crawl          | Common Crawl ID override                            |
| `-files-limit`  | 0                     | Cap number of WET files (fixed workload)           |
| `-chunks-limit` | 0                     | Second workload cap kept for compatibility         |
| `-nodes`        | `1 2 4 8 16 32 64 128`| Space-separated node counts to sweep               |
| `-reps`         | 3                     | Timed runs per node count (median reported)        |
| `-port`         | 9090                  | Worker HTTP port                                    |
| `-out`          | `experiments/results.csv` | Output CSV path                                |
| `-plot`         | _(off)_               | Render `experiments/amdahl.png` after the sweep    |
| `-no-fetch`     | _(off)_               | Reuse `experiments/bench-hosts.txt` (skip `make_hosts`) |

> **Download note:** Common Crawl files are downloaded by workers during `/load`,
> before the timer starts, so the benchmark still measures map → reduce → collect
> rather than website download time.

> **Methodology note:** each run uses exactly N active workers and **no spares**,
> so the measurements reflect pure compute scaling. A run is discarded (not
> recorded) if it fails or if the orchestrator reports a different active node
> count than requested (e.g. fewer healthy workers); the median is taken over the
> surviving runs. The data-load/upload step runs *before* the timer, so only the
> map → reduce → collect compute time contributes to the speedup.

---

## Error Handling & Fault Tolerance

The orchestrator uses a **slot-based coordinator** with a **spare pool**:

- `N` (target `-n`) logical slots map 1:1 to physical hosts. Partitioning
  uses `FNV-32a(key) % N`, so `N` is fixed for the duration of the job.
- Hosts beyond `N` in `hosts.txt` become **spares** — kept healthy and
  ready to replace any slot whose host fails.
- Every worker call (`/load`, `/map`, `/reduce`, `/result`) is bound to a
  `context.Context` so it can be cancelled promptly.
- A background **health watcher** polls `GET /health` on every active and
  spare host every `-health-interval` (default 5 s). Two consecutive failures
  mark a host dead so it will not be reused.

| Scenario                            | Behaviour                                                                                                          |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| Worker not reachable at startup     | `waitHealthy` retries 30× / 2 s; survivors form slot+spare pool                                                    |
| SCP / SSH failure (deploy)          | Failed hosts are skipped; deployment continues with the hosts that came up successfully                             |
| Worker returns 5xx during map/reduce | Calling host is replaced from spare pool, slot rewinds to `pending`, `/load` → `/map` (→ `/reduce`) replay        |
| Worker dies → peer's `/reduce` 5xx with `fetch from peer X` | Coordinator parses the message, blames host `X` (not the caller), replaces `X`, then re-runs `/reduce` on all surviving slots with the updated peer list |
| Transport error / timeout on a slot | Same as above; calling host replaced                                                                               |
| Map/reduce phase times out          | 60 min HTTP client timeout per worker; failure triggers slot replacement                                            |
| Data upload times out               | 30 min HTTP client timeout for `/load`; failure triggers slot replacement                                          |
| `/intermediate` fetch failure       | 10 min worker-internal timeout; worker returns 500 to its `/reduce` caller; orchestrator identifies the dead peer  |
| `run.sh` receives SIGINT/SIGTERM    | `trap cleanup EXIT` kills remote processes                                                                          |

### Tunable flags (orchestrator)

| Flag                  | Default | Purpose                                                       |
| --------------------- | ------- | ------------------------------------------------------------- |
| `-n`                  | 0       | Target number of active slots; extras in `hosts.txt` are spares |
| `-commoncrawl`        | false   | Use the official Common Crawl website instead of a local file |
| `-crawl`              | latest  | Override the Common Crawl ID for deterministic runs           |
| `-files-limit`        | 0       | Cap the number of Common Crawl WET files selected             |
| `-chunks-limit`       | 0       | Second Common Crawl workload cap kept for compatibility       |
| `-max-attempts`       | 4       | Per-slot host swaps before failing the job                    |
| `-health-interval`    | 5s      | `/health` poll cadence during long phases                     |
| `-backoff-initial`    | 250ms   | First retry backoff (doubles, capped at 5 s)                  |

### When the job will still fail

- Spare pool exhausted (more slots fail than spares can replace).
- All workers fail their initial `/health` check.
- A single slot trips `-max-attempts` swaps.
