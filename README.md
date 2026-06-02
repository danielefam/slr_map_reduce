# MapReduce V1 And Distributed Load Monitor

This repository now contains a first MapReduce implementation for word count, inspired by the architecture in the Dean and Ghemawat paper. The MapReduce path is the main distributed-processing implementation in the project:

- `Main` is a local coordinator.
- `N0`, `N1`, `N2`, ... are remote workers.
- map output is written on worker-local disk under `/tmp`, not under `$HOME` on NFS.
- reduce ownership uses `hash(key) % number_of_nodes`.
- reduce starts only after all map tasks complete.

The original distributed load-monitor assignment is still present and works as a separate flow.

## Project Focus

There are now two independent distributed flows in the repository:

1. MapReduce V1 for distributed word count.
2. The original load-average monitor.

If you want to exercise the distributed-systems work added most recently, start with the MapReduce section below.

## MapReduce V1

The current implementation is intentionally simple and word-count-specific:

- `bin/mr_coordinator` assigns map tasks, enforces the map barrier, starts reduce tasks, and gathers final output.
- `bin/mr_worker` executes map tasks, stores intermediate partitions, serves shuffle fetches, and writes reducer output.
- `scripts/deploy_mapreduce.sh` deploys workers.
- `scripts/run_mapreduce.sh` runs the coordinator locally.
- `scripts/clean_mapreduce.sh` removes both deployment state and worker-local scratch data.

See [docs/mapreduce_v1.md](docs/mapreduce_v1.md) for the protocol, architecture notes, and the space-time diagram.
See [docs/ADAPTIVE_EXECUTION.md](docs/ADAPTIVE_EXECUTION.md) for how mapper/reducer counts are selected dynamically.
See [docs/EXIT_CODES.md](docs/EXIT_CODES.md) for a stable, machine-readable failure taxonomy.
See [docs/NEXT_STEPS_BLUEPRINT.md](docs/NEXT_STEPS_BLUEPRINT.md) for the implementation roadmap (bootstrap protocol v2, robust reduce remote-read, and Common Crawl integration).

## Word Count Example

Use the sample input file [examples/wordcount_input.txt](/home/daniele/Desktop/university/slr_map_reduce/examples/wordcount_input.txt) to test the MapReduce path. The expected result is in [examples/wordcount_expected.txt](/home/daniele/Desktop/university/slr_map_reduce/examples/wordcount_expected.txt).

Example local run after workers are available:

```bash
make run-mr MR_INPUT=examples/wordcount_input.txt MR_OUTPUT=run/mr_wordcount.txt MR_CHUNK_LINES=2
```

Expected output:

```text
blue	2
fox	2
green	3
hare	1
red	3
swift	1
turtle	1
```

## MapReduce Workflow

### Deploy MapReduce Workers

```bash
make deploy-mr HOST_COUNT=3
```

`HOST_COUNT=3` is only a small demo value for quick tests. Use any count you want (up to the available hosts), or run `make deploy-mr` to use the default.

Useful overrides:

```bash
make deploy-mr HOST_COUNT=3 MR_MANIFEST=run/mr_manifest.env MR_REMOTE_DIRNAME=my_mr_bundle
```

The deploy script starts one `mr_worker` per host and stores its local scratch root in the manifest so cleanup can remove job data safely. It first uploads the worker bundle through the staging host, then falls back to a per-host upload if a selected node cannot see the shared bundle.

### Run A Word Count Job

```bash
make run-mr MR_INPUT=examples/wordcount_input.txt MR_OUTPUT=run/mr_wordcount.txt MR_CHUNK_LINES=2
```

The coordinator runs locally, reads the worker host list from the manifest, assigns map work, waits for all maps to finish, then starts the reduce phase.
By default it uses only the effective mapper/reducer count needed by the input splits. To cap reducers explicitly, pass `MR_REDUCERS=N`.

### Clean MapReduce Deployment State

```bash
make clean-remote-mr
```

This stops the current MapReduce workers, removes the shared deployment bundle, and removes the deployment-scoped local scratch directory under `/tmp/slr_map_reduce` on each worker.

## Components

- `bin/load_server`: C TCP server. It listens on one shared port, reads `/proc/loadavg`, sends a single line `load1 load5 load15`, then closes the connection.
- `bin/load_client`: C TCP client. It reads the selected host list, connects to each server, parses the one-line response, and prints the average load across the responding hosts.
- `bin/mr_worker`: C TCP worker for the MapReduce word-count flow. It runs map tasks, stores intermediate partitions on local scratch storage, serves shuffle fetches, and executes reduce tasks.
- `bin/mr_coordinator`: C coordinator for the MapReduce word-count flow. It assigns map tasks, enforces the map-to-reduce barrier, launches reduce work, and fetches final outputs.
- `scripts/select_hosts.sh`: performs the same `GET https://tp.telecom-paris.fr/ajax.php` request used by the browser, filters alive hosts, appends `.enst.fr`, and writes a deterministic 100-host file.
- `scripts/select_port.sh`: uses one SSH probe per host to collect currently listening TCP ports, then selects the first common free port in the configured range.
- `scripts/deploy.sh`: builds locally, copies the server bundle once through the first selected host, then launches the server on each selected host.
- `scripts/deploy_mapreduce.sh`: builds locally, copies the MapReduce worker bundle once, then launches one worker on each selected host.
- `scripts/status.sh`: checks whether the distributed servers are running and listening, repairs stale/missing pid files when possible, and reports unhealthy anomalies (for example listening-without-pid).
- `scripts/stop.sh`: stops the distributed servers.
- `scripts/clean_remote.sh`: stops the distributed servers for the current manifest and removes the current shared remote bundle, logs, and pid files.
- `scripts/clean_mapreduce.sh`: stops the distributed MapReduce workers, removes the current shared remote bundle, and removes worker-local scratch data under `/tmp/slr_map_reduce/<deployment>`.
- `scripts/run_client.sh`: runs the local aggregator client using the stored manifest.
- `scripts/run_mapreduce.sh`: runs the local coordinator using the stored worker manifest and writes the final word-count output locally.
- `scripts/run_mapreduce.sh`: runs the local coordinator using the stored worker manifest and writes the final word-count output locally.
- `scripts/test_mapreduce_local.sh`: runs a complete end-to-end MapReduce word-count test on localhost with one worker and one coordinator, validating the protocol and output.
- `scripts/smoke_local.sh`: local one-host smoke test.

Status commands:

```bash
make status
```

Informational health report for day-to-day use. It never fails the Make target.

```bash
make status-strict
```

Strict health check. `stopped` is informational, but anomalies such as `listening-without-pid` still return non-zero.

`make status-strict` now retries automatically for transient issues (for example DNS hiccups).
Default retry policy:
- `STATUS_RETRIES=2`
- `STATUS_RETRY_DELAY=3` seconds

Override example:

```bash
make status-strict STATUS_RETRIES=4 STATUS_RETRY_DELAY=5
```

`make status-report` is an alias of `make status`.

Automatic remediation command:

```bash
make remediate-status HOST_COUNT=3
```

Dry-run version (no remote side effects):

```bash
make remediate-status-dry HOST_COUNT=3
```

This command runs a robust recovery loop for the legacy load-server flow:
1. status report
2. remote cleanup
3. host reselection with alternative nodes
4. common-port reselection
5. redeploy and recheck

It writes logs to `run/remediation.log` and `run/remediation_status_report.txt`.

Useful direct options:

```bash
bash scripts/remediate_status.sh --manifest run/manifest.env --host-count 3 --retries 3 --minimum-host-count 2
```

Pass extra options from Make via `REMEDIATE_ARGS`, for example:

```bash
make remediate-status HOST_COUNT=3 REMEDIATE_ARGS="--retries 3 --minimum-host-count 2 --clean-all"
```

Use `--clean-all` if you also want MapReduce cleanup during each retry cycle.
- `docs/mapreduce_v1.md`: protocol, architecture, word-count example, and the first space-time diagram.

## Prerequisites

- Linux environment with `/proc/loadavg`
- `cc`, `make`, `curl`, `jq`, `ssh`, `scp`
- Passwordless SSH access to the university hosts
- Shared NFS-backed home directories across the remote machines

## Build

```bash
make
```

## Local Smoke Test

This validates the C binaries and the end-to-end protocol on localhost.

```bash
make smoke
```

## Local MapReduce Test

To test the MapReduce word-count implementation end-to-end on localhost without needing remote worker deployment:

```bash
bash scripts/test_mapreduce_local.sh
```

This script:
1. Starts a single `mr_worker` on port 20123 on localhost
2. Runs the `mr_coordinator` with the sample input file
3. Executes the complete map and reduce phases locally
4. Verifies the output matches the expected word count result

This is useful for validating the MapReduce protocol and word-count logic before attempting distributed deployment across remote hosts.

## Legacy Load-Monitor Workflow (Deprecated)

Attention: this section is deprecated for current project work.
Use the MapReduce workflow above for deployment and experiments.
The legacy targets are kept only for backward compatibility with the old assignment.

The sections below describe the original distributed load-average monitor flow that remains in the repository as a separate assignment.

## Full Deployment Flow

1. Select 100 alive hosts from `https://tp.telecom-paris.fr/ajax.php` and write them as canonical names such as `tp-1a201-05.enst.fr`.
2. Discover one common free port across those hosts.
3. Build the server and client locally.
4. Copy the server bundle once to the first selected host with `scp`. Because the remote home is NFS-shared, every selected machine can see the same bundle.
5. Issue one `ssh` command per selected host to start the server.
6. Run the client locally against the generated host list and selected port.

To create the canonical 100-host file explicitly:

```bash
make select-hosts HOST_COUNT=100
```

The default deployment command is:

```bash
make deploy
```

Useful overrides:

```bash
make deploy HOST_COUNT=100 PORT_START=21000 PORT_END=21099 REMOTE_DIRNAME=my_bundle
```

If you already have a hosts file, skip dynamic selection and deploy directly with:

```bash
bash scripts/deploy.sh --hosts run/hosts.txt --manifest run/manifest.env
```

When `--hosts` is provided, deployment now continues with the reachable subset of that file as long as at least one host is reachable. The filtered reachable host list is written back through the manifest so `status`, `stop`, and `run-client` operate on the same set.

## Check Status

```bash
make status
```

## Run the Client

```bash
make run-client
```

The client exits with status `0` only when every host replies successfully. If some hosts fail, it still prints the average over successful replies and exits non-zero.

## Stop the Servers

```bash
make stop
```

`make stop` is a lightweight shutdown that targets the current manifest hosts and port.

## Clean Remote Deployment State

```bash
make clean-remote
```

This stops the current deployment and removes the current remote bundle directory, logs, and pid files. Use it before redeploying updated server binaries when you want to avoid stale remote state.

## Clean and Redeploy

```bash
make redeploy
```

This runs the remote cleanup first, then performs a fresh deploy.

## Runtime Files

- `run/hosts.txt`: selected host list
- `run/manifest.env`: selected port and deployment metadata
- `run/launch_status.tsv`: per-host launch result from the last deployment
- `run/mr_manifest.env`: selected worker port and MapReduce deployment metadata
- `run/mr_job.env`: latest local MapReduce job metadata
- `run/mr_wordcount.txt`: default local MapReduce output file

## Notes

- The browser uses a plain `GET https://tp.telecom-paris.fr/ajax.php` request with no extra query parameters or form submission. The JSON payload is shaped as `{"data": [[host, alive, local_sessions, remote_sessions, ssh_sessions], ...]}`.
- The default common-port search range is `20000-20099`. Change it if your cluster uses that range already.
- For manual recovery, you can prepare `run/hosts.txt` yourself and then run `scripts/select_port.sh`, `scripts/deploy.sh`, and `scripts/run_client.sh` directly.
- The current MapReduce implementation has been validated locally on `localhost`. Remote validation depends on SSH access to the target hosts and has not been completed from this environment.