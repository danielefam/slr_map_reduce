# MapReduce Exit Codes

This document defines stable exit codes for the MapReduce coordinator path.

Scope:
- `bin/mr_coordinator`
- `scripts/run_mapreduce.sh`
- Any automation that depends on MapReduce job status

## Exit Code Matrix

| Code | Name | Meaning | Typical Action |
|---:|---|---|---|
| 0 | `MR_EXIT_SUCCESS` | Job completed and output was written | No action |
| 2 | `MR_EXIT_USAGE` | Invalid CLI arguments or required parameters missing | Fix invocation/flags |
| 3 | `MR_EXIT_HOSTS` | Hosts file/manifest could not be loaded or had no valid hosts | Regenerate manifest/hosts |
| 4 | `MR_EXIT_INPUT` | Input file could not be read or split | Check input path/permissions |
| 5 | `MR_EXIT_SETUP` | Job setup failed (e.g., job id generation or allocations) | Retry once, then inspect logs |
| 6 | `MR_EXIT_NETWORK` | Network/connect/send/recv timeout or remote I/O failure | Retry deployment/run; inspect remote reachability |
| 7 | `MR_EXIT_PROTOCOL` | Remote response format/command contract mismatch | Check worker/coordinator compatibility |
| 8 | `MR_EXIT_OUTPUT` | Final output file could not be written | Check output path/permissions |
| 9 | `MR_EXIT_INTERNAL` | Unexpected internal failure | Inspect coordinator logs and open issue |

## Error Classification Rules

The coordinator classifies remote failures into two groups:

1. `MR_EXIT_NETWORK`:
- connect failed
- socket send/recv failed
- timed out while waiting on remote endpoint

2. `MR_EXIT_PROTOCOL`:
- remote response does not start with expected `OK ...` for control commands
- remote response does not match `DATA <bytes>` for result fetch

## Script Behavior

`scripts/run_mapreduce.sh` propagates the exact coordinator exit code and prints a human-readable category:

- Example: `MapReduce coordinator failed (exit=6: network/remote I/O error)`

This allows both humans and automation to reason on the same error contract.

## Compatibility Notes

- Keep numeric values stable once consumed by CI or orchestration scripts.
- New categories should use new numbers; do not repurpose existing ones.
- If future protocol versions add richer status payloads, preserve these top-level exit groups for backward compatibility.
