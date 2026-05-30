# Next Steps Blueprint

This blueprint defines the implementation order after the current word-count path is stable under failures.

## Hard Gates Before New Features

All items below must pass before enabling protocol v2 features:

1. Deterministic output gate:
- Same input + same worker set => byte-identical output on repeated runs.

2. Failure semantics gate:
- Coordinator returns documented exit codes for injected failures.

3. Adaptive execution gate:
- If fewer workers are available than requested, effective mapper/reducer counts are adjusted by policy and recorded.
- Current implementation covers input-size-based adaptation; future work should replace the worker pool with a live worker set from health checks. See `docs/ADAPTIVE_EXECUTION.md`.

4. Operational gate:
- Deploy/run/clean scripts preserve machine-readable failure reasons.

## Bootstrap Protocol (v2) - Design Target

The first message before `MAP` should be a bootstrap handshake that carries:

- protocol version
- job id
- timeout and retry policy
- effective worker set
- effective reducer count
- feature flags

### Expected behavior

1. Coordinator sends bootstrap to all workers.
2. Worker validates compatibility.
3. Worker ACKs or rejects with explicit reason.
4. Coordinator starts map phase only after successful bootstrap quorum policy.

## Reduce Remote Read Contract

Map outputs remain on local disks at workers (Dean/Ghemawat model alignment).

### Required properties

1. Reducer can discover mapper partition locations.
2. Fetch is stream-oriented (no full-file memory load requirement).
3. Data integrity can be validated (size/checksum metadata).
4. Missing mapper partitions are handled by declared failure policy (retry/fail-fast).

### Operational implications

- Track per-fetch failure reason and retries.
- Keep remote paths job-scoped under `/tmp/slr_map_reduce/...`.
- Keep cleanup precise and safe for job-scoped artifacts.

## Common Crawl Integration Plan (/cal/commoncrawl)

Start only with word frequency.

### Stage A: Readiness

1. Verify mount and permissions for `/cal/commoncrawl`.
2. Implement split discovery and filtering policy.
3. Define record-boundary-safe split handling.

### Stage B: Execution

1. Run word frequency on a small curated subset of splits.
2. Validate output determinism and exit-code behavior under induced failures.
3. Scale progressively and monitor shuffle pressure.

## Long-Term Appendix (Not Implemented Now)

1. Fault tolerance protocol evolution:
- richer task lifecycle states
- checkpoint/restart semantics
- resumable reduce operations

2. Kafka Streams comparison track:
- run the same word-frequency workload on same splits
- compare correctness, latency, operational complexity
- account for non-root deployment constraints on school machines

## Documentation Deliverables

To keep implementation and operations aligned, maintain these docs as features land:

1. `docs/EXIT_CODES.md` (authoritative failure taxonomy)
2. `docs/ADAPTIVE_EXECUTION.md` (effective mapper/reducer policy)
3. Protocol v2 spec (bootstrap + compatibility)
4. Reduce remote-read spec (streaming and integrity)
5. Common Crawl runbook (`/cal/commoncrawl` lifecycle)
6. Troubleshooting matrix with exact exit codes and actions
