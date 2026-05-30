# Adaptive MapReduce Execution

This document describes how the current word-count implementation chooses effective mapper and reducer counts before future fault-tolerance work lands.

## Goals

The coordinator should avoid launching unnecessary work when the input is small compared to the worker pool.

Examples:
- 1 input split and 100 deployed workers should not require 100 reducers.
- 3 input splits and 100 deployed workers should use only the workers that can actually receive map work.
- Final output must remain deterministic because the coordinator globally sorts reducer outputs before writing the final file.

## Current Policy

Let:

- `worker_pool_count` = number of hosts in the MapReduce manifest
- `map_task_count` = number of input chunks produced by `--chunk-lines`
- `requested_reducers` = optional `--reducers N` limit

The coordinator computes:

```text
effective_mapper_count = min(map_task_count, worker_pool_count)
effective_reducer_count = effective_mapper_count
if requested_reducers is set:
    effective_reducer_count = min(effective_reducer_count, requested_reducers)
```

Both effective counts are always at least 1 because an empty input still creates one empty map chunk.

## Mapper Assignment

Map tasks are assigned round-robin across the effective mapper set:

```text
mapper_host = hosts[task_id % effective_mapper_count]
```

If the manifest contains more workers than needed, idle workers are not contacted during this job.

## Reducer Assignment

Reducers are assigned to the first `effective_reducer_count` hosts in the manifest.

Each mapper partitions keys using:

```text
reducer_id = hash(key) % effective_reducer_count
```

This keeps all occurrences of a word routed to exactly one reducer.

## Shuffle Peers

Reducers fetch map partitions only from workers that actually received map tasks.
Because map tasks are assigned round-robin starting at `hosts[0]`, the active mappers
are always the first `effective_mapper_count` hosts in the manifest:

```text
peers = hosts[0..effective_mapper_count-1]
```

This avoids needless fetches from idle workers that never received a map task.

## Job Manifest Fields

The coordinator writes these fields to `run/mr_job.env`:

- `MAP_TASK_COUNT`
- `WORKER_POOL_COUNT`
- `EFFECTIVE_MAPPER_COUNT`
- `EFFECTIVE_REDUCER_COUNT`
- `PORT`

These values make the execution policy auditable after the job finishes.

## Operator Overrides

Default automatic policy:

```bash
make run-mr MR_INPUT=examples/wordcount_input.txt
```

Limit reducers explicitly:

```bash
make run-mr MR_INPUT=examples/wordcount_input.txt MR_REDUCERS=2
```

Or use the script directly:

```bash
bash scripts/run_mapreduce.sh --manifest run/mr_manifest.env --input examples/wordcount_input.txt --reducers 2
```

## Future Fault-Tolerance Extension

This policy is intentionally simple. Later failure-aware scheduling should extend it by replacing `worker_pool_count` with a live worker set computed from health checks and bootstrap negotiation.

The intended future formula is:

```text
effective_mapper_count = min(map_task_count, live_worker_count)
effective_reducer_count = min(effective_mapper_count, requested_reducers_or_live_worker_count)
```
