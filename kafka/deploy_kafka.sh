#!/usr/bin/env bash
# Deploys an Apache Kafka (KRaft, no ZooKeeper) cluster onto a subset of lab
# machines from hosts.txt — WITHOUT root privileges and WITHOUT Docker.
#
# Layout per node (all on node-local disk, not NFS):
#   /tmp/kafka-bench-<user>/kafka_<scala>-<version>/   extracted distribution
#   /tmp/kafka-bench-<user>/logs/                      Kafka log.dirs
#   /tmp/kafka-bench-<user>/kafka.pid                  broker PID
#
# Topology: node 1 = combined broker+controller, nodes 2..N = brokers
# (static KRaft quorum with a single controller — sufficient for benchmarks).
#
# Usage:
#   ./deploy_kafka.sh [-n 3] [-hosts ../hosts.txt] [-version 4.3.0] [-port 9092] [-controller-port 9093]
#
# Requires: Java 17+ on the remote machines (checked), ssh/scp key access.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

N=3
HOSTS_FILE="$SCRIPT_DIR/../hosts.txt"
KAFKA_VERSION="4.3.0"
SCALA_VERSION="2.13"
BROKER_PORT=9092
CONTROLLER_PORT=9093
REMOTE_ROOT="/tmp/kafka-bench-${USER:-$(id -un)}"
SSH_OPTS=(-o BatchMode=yes -o ConnectTimeout=10 -o ServerAliveInterval=10 -o ServerAliveCountMax=3)
SCP_OPTS=(-o BatchMode=yes -o ConnectTimeout=10 -o ServerAliveInterval=10 -o ServerAliveCountMax=3)
DEPLOY_PARALLELISM="${KAFKA_DEPLOY_PARALLELISM:-8}"
REMOTE_DOWNLOAD="${KAFKA_REMOTE_DOWNLOAD:-0}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    -n)        N="$2";               shift 2 ;;
    -hosts)    HOSTS_FILE="$2";      shift 2 ;;
    -version)  KAFKA_VERSION="$2";   shift 2 ;;
    -port)     BROKER_PORT="$2";     shift 2 ;;
    -controller-port) CONTROLLER_PORT="$2"; shift 2 ;;
    *) echo "Unknown flag: $1" >&2; exit 1 ;;
  esac
done

KAFKA_DIST="kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
TARBALL="${KAFKA_DIST}.tgz"
DIST_CACHE="$SCRIPT_DIR/dist"
PRIMARY_URL="https://dlcdn.apache.org/kafka/${KAFKA_VERSION}/${TARBALL}"
ARCHIVE_URL="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${TARBALL}"
KAFKA_HOSTS_FILE="$SCRIPT_DIR/kafka_hosts.txt"
CLUSTER_ENV_FILE="$SCRIPT_DIR/kafka_cluster.env"

# ── Step 1: pick the first N hosts ──────────────────────────────────────────
mapfile -t ALL_HOSTS < <(grep -v '^[[:space:]]*$' "$HOSTS_FILE")
if (( ${#ALL_HOSTS[@]} < N )); then
  echo "ERROR: need $N hosts but $HOSTS_FILE only has ${#ALL_HOSTS[@]}" >&2
  exit 1
fi
HOSTS=("${ALL_HOSTS[@]:0:$N}")
printf '%s\n' "${HOSTS[@]}" > "$KAFKA_HOSTS_FILE"
CONTROLLER_HOST="${HOSTS[0]}"
echo "=== Deploying Kafka $KAFKA_VERSION to $N nodes (controller: $CONTROLLER_HOST) ==="

# ── Step 2: download the distribution once, locally ────────────────────────
mkdir -p "$DIST_CACHE"
if [[ ! -f "$DIST_CACHE/$TARBALL" ]]; then
  echo "--- Downloading $TARBALL ---"
  curl -fL --retry 3 -o "$DIST_CACHE/$TARBALL.part" "$PRIMARY_URL" \
    || curl -fL --retry 3 -o "$DIST_CACHE/$TARBALL.part" "$ARCHIVE_URL"
  mv "$DIST_CACHE/$TARBALL.part" "$DIST_CACHE/$TARBALL"
else
  echo "--- Using cached $TARBALL ---"
fi

# ── Step 3: verify Java >= 17 on every node ─────────────────────────────────
echo "--- Checking Java on remote nodes ---"
for h in "${HOSTS[@]}"; do
  ver=$(ssh "${SSH_OPTS[@]}" "$h" \
    "java -version 2>&1 | head -1" || echo "none")
  echo "  $h: $ver"
  if ! ssh "${SSH_OPTS[@]}" "$h" \
      'v=$(java -version 2>&1 | sed -nE "s/.*version \"([0-9]+).*/\1/p"); [[ -n "$v" && "$v" -ge 17 ]]'; then
    echo "ERROR: $h lacks Java 17+ (Kafka 4.x requirement)" >&2
    exit 1
  fi
done

# ── Step 4: upload/download + extract on every node (node-local /tmp) ───────
if [[ "$REMOTE_DOWNLOAD" == "1" ]]; then
  echo "--- Downloading and extracting distribution on remote nodes ---"
else
  echo "--- Uploading and extracting distribution ---"
fi
pids=()
for h in "${HOSTS[@]}"; do
  (
    echo "  $h"
    ssh "${SSH_OPTS[@]}" "$h" "mkdir -p $REMOTE_ROOT"
    if [[ "$REMOTE_DOWNLOAD" == "1" ]]; then
      ssh "${SSH_OPTS[@]}" "$h" \
        "cd $REMOTE_ROOT && if [[ ! -d $KAFKA_DIST ]]; then rm -f $TARBALL.part; timeout 300 curl -fsL --retry 3 -o $TARBALL.part $PRIMARY_URL || timeout 300 curl -fsL --retry 3 -o $TARBALL.part $ARCHIVE_URL; mv $TARBALL.part $TARBALL; fi"
    else
      scp "${SCP_OPTS[@]}" -q "$DIST_CACHE/$TARBALL" "$h:$REMOTE_ROOT/$TARBALL"
    fi
    ssh "${SSH_OPTS[@]}" "$h" \
      "cd $REMOTE_ROOT && if [[ ! -d $KAFKA_DIST ]]; then tar xzf $TARBALL && rm -f $TARBALL; fi"
  ) &
  pids+=("$!")
  if (( ${#pids[@]} >= DEPLOY_PARALLELISM )); then
    wait "${pids[0]}"
    pids=("${pids[@]:1}")
  fi
done
for pid in "${pids[@]}"; do
  wait "$pid"
done

# ── Step 5: generate one cluster ID (on the controller node) ───────────────
CLUSTER_ID=$(ssh "${SSH_OPTS[@]}" "$CONTROLLER_HOST" \
  "$REMOTE_ROOT/$KAFKA_DIST/bin/kafka-storage.sh random-uuid" | tr -d '[:space:]')
echo "--- Cluster ID: $CLUSTER_ID ---"

# ── Step 6: write per-node config, format storage, start broker ────────────
QUORUM="1@${CONTROLLER_HOST}:${CONTROLLER_PORT}"
node_id=1
for h in "${HOSTS[@]}"; do
  echo "--- Configuring node $node_id ($h) ---"
  if (( node_id == 1 )); then
    ROLES="broker,controller"
    LISTENERS="PLAINTEXT://0.0.0.0:${BROKER_PORT},CONTROLLER://0.0.0.0:${CONTROLLER_PORT}"
  else
    ROLES="broker"
    LISTENERS="PLAINTEXT://0.0.0.0:${BROKER_PORT}"
  fi

  ssh "${SSH_OPTS[@]}" "$h" "cat > $REMOTE_ROOT/server.properties" <<EOF
process.roles=${ROLES}
node.id=${node_id}
controller.quorum.voters=${QUORUM}
listeners=${LISTENERS}
advertised.listeners=PLAINTEXT://${h}:${BROKER_PORT}
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
inter.broker.listener.name=PLAINTEXT
log.dirs=${REMOTE_ROOT}/logs
num.partitions=${N}
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
EOF

  ssh "${SSH_OPTS[@]}" "$h" "
    set -e
    cd $REMOTE_ROOT/$KAFKA_DIST
    rm -rf $REMOTE_ROOT/logs
    bin/kafka-storage.sh format -t $CLUSTER_ID -c $REMOTE_ROOT/server.properties >/dev/null
    nohup bin/kafka-server-start.sh $REMOTE_ROOT/server.properties \
      > $REMOTE_ROOT/kafka.log 2>&1 &
    echo \$! > $REMOTE_ROOT/kafka.pid
  "

  if (( node_id == 1 )); then
    echo "--- Waiting for controller on $h ---"
    controller_ok=0
    broker_ok=0
    for _ in $(seq 1 60); do
      if ssh "${SSH_OPTS[@]}" "$h" "bash -c 'echo > /dev/tcp/127.0.0.1/${CONTROLLER_PORT}'" 2>/dev/null; then
        controller_ok=1
      fi
      if ssh "${SSH_OPTS[@]}" "$h" "bash -c 'echo > /dev/tcp/127.0.0.1/${BROKER_PORT}'" 2>/dev/null; then
        broker_ok=1
      fi
      if (( controller_ok && broker_ok )); then
        break
      fi
      sleep 2
    done
    if ! (( controller_ok && broker_ok )); then
      echo "ERROR: controller node $h did not become ready; see $REMOTE_ROOT/kafka.log" >&2
      exit 1
    fi
  fi

  node_id=$((node_id + 1))
done

# ── Step 7: health check ────────────────────────────────────────────────────
echo "--- Waiting for brokers to come up ---"
for h in "${HOSTS[@]}"; do
  ok=0
  for _ in $(seq 1 30); do
    if ssh "${SSH_OPTS[@]}" "$h" "bash -c 'echo > /dev/tcp/127.0.0.1/${BROKER_PORT}'" 2>/dev/null; then
      ok=1; break
    fi
    sleep 2
  done
  if (( ok )); then
    echo "  ✓ $h:${BROKER_PORT} is accepting connections"
  else
    echo "  ✗ $h:${BROKER_PORT} did not come up; see $REMOTE_ROOT/kafka.log on that host" >&2
    exit 1
  fi
done

echo ""
echo "Kafka cluster ready. Bootstrap server: ${CONTROLLER_HOST}:${BROKER_PORT}"
echo "Hosts written to $KAFKA_HOSTS_FILE"
cat > "$CLUSTER_ENV_FILE" <<EOF
BROKER_PORT=$BROKER_PORT
CONTROLLER_PORT=$CONTROLLER_PORT
KAFKA_VERSION=$KAFKA_VERSION
SCALA_VERSION=$SCALA_VERSION
REMOTE_ROOT=$REMOTE_ROOT
EOF
echo "Cluster settings written to $CLUSTER_ENV_FILE"
echo "Next: ./run_kafka_wordcount.sh -input <file> [-output kafka_result.txt]"
