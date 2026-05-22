# Distributed Load Average Monitor

This project deploys one TCP server on each of 100 Linux hosts, using exactly one `scp` to copy the server bundle into the shared NFS-backed home directory and one `ssh` launch command per host. A local client then connects to every server, collects the 1-minute, 5-minute, and 15-minute CPU load averages, and prints the cluster-wide average.

## Components

- `bin/load_server`: C TCP server. It listens on one shared port, reads `/proc/loadavg`, sends a single line `load1 load5 load15`, then closes the connection.
- `bin/load_client`: C TCP client. It reads the selected host list, connects to each server, parses the one-line response, and prints the average load across the responding hosts.
- `scripts/select_hosts.sh`: performs the same `GET https://tp.telecom-paris.fr/ajax.php` request used by the browser, filters alive hosts, appends `.enst.fr`, and writes a deterministic 100-host file.
- `scripts/select_port.sh`: uses one SSH probe per host to collect currently listening TCP ports, then selects the first common free port in the configured range.
- `scripts/deploy.sh`: builds locally, copies the server bundle once through the first selected host, then launches the server on each selected host.
- `scripts/status.sh`: checks whether the distributed servers are running and listening.
- `scripts/stop.sh`: stops the distributed servers.
- `scripts/clean_remote.sh`: stops the distributed servers for the current manifest and removes the current shared remote bundle, logs, and pid files.
- `scripts/run_client.sh`: runs the local aggregator client using the stored manifest.
- `scripts/smoke_local.sh`: local one-host smoke test.

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

## Notes

- The browser uses a plain `GET https://tp.telecom-paris.fr/ajax.php` request with no extra query parameters or form submission. The JSON payload is shaped as `{"data": [[host, alive, local_sessions, remote_sessions, ssh_sessions], ...]}`.
- The default common-port search range is `20000-20099`. Change it if your cluster uses that range already.
- For manual recovery, you can prepare `run/hosts.txt` yourself and then run `scripts/select_port.sh`, `scripts/deploy.sh`, and `scripts/run_client.sh` directly.