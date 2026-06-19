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
#   ./deploy_kafka.sh [-n 3] [-hosts ../hosts.txt] [-version 4.3.0]
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

while [[ $# -gt 0 ]]; do
  case "$1" in
    -n)        N="$2";               shift 2 ;;
    -hosts)    HOSTS_FILE="$2";      shift 2 ;;
    -version)  KAFKA_VERSION="$2";   shift 2 ;;
    -port)     BROKER_PORT="$2";     shift 2 ;;
    *) echo "Unknown flag: $1" >&2; exit 1 ;;
  esac
done

KAFKA_DIST="kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
TARBALL="${KAFKA_DIST}.tgz"
DIST_CACHE="$SCRIPT_DIR/dist"
PRIMARY_URL="https://dlcdn.apache.org/kafka/${KAFKA_VERSION}/${TARBALL}"
ARCHIVE_URL="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${TARBALL}"
KAFKA_HOSTS_FILE="$SCRIPT_DIR/kafka_hosts.txt"

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
  ver=$(ssh -o BatchMode=yes "$h" \
    "java -version 2>&1 | head -1" || echo "none")
  echo "  $h: $ver"
  if ! ssh -o BatchMode=yes "$h" \
      'v=$(java -version 2>&1 | sed -nE "s/.*version \"([0-9]+).*/\1/p"); [[ -n "$v" && "$v" -ge 17 ]]'; then
    echo "ERROR: $h lacks Java 17+ (Kafka 4.x requirement)" >&2
    exit 1
  fi
done

# ── Step 4: upload + extract on every node (node-local /tmp) ────────────────
echo "--- Uploading and extracting distribution ---"
for h in "${HOSTS[@]}"; do
  echo "  $h"
  ssh -o BatchMode=yes "$h" "mkdir -p $REMOTE_ROOT"
  scp -q "$DIST_CACHE/$TARBALL" "$h:$REMOTE_ROOT/$TARBALL"
  ssh -o BatchMode=yes "$h" \
    "cd $REMOTE_ROOT && rm -rf $KAFKA_DIST && tar xzf $TARBALL && rm -f $TARBALL"
done

# ── Step 5: generate one cluster ID (on the controller node) ───────────────
CLUSTER_ID=$(ssh -o BatchMode=yes "$CONTROLLER_HOST" \
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

  ssh -o BatchMode=yes "$h" "cat > $REMOTE_ROOT/server.properties" <<EOF
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

  ssh -o BatchMode=yes "$h" "
    set -e
    cd $REMOTE_ROOT/$KAFKA_DIST
    rm -rf $REMOTE_ROOT/logs
    bin/kafka-storage.sh format -t $CLUSTER_ID -c $REMOTE_ROOT/server.properties >/dev/null
    nohup bin/kafka-server-start.sh $REMOTE_ROOT/server.properties \
      > $REMOTE_ROOT/kafka.log 2>&1 &
    echo \$! > $REMOTE_ROOT/kafka.pid
  "
  node_id=$((node_id + 1))
done

# ── Step 7: health check ────────────────────────────────────────────────────
echo "--- Waiting for brokers to come up ---"
for h in "${HOSTS[@]}"; do
  ok=0
  for _ in $(seq 1 30); do
    if ssh -o BatchMode=yes "$h" "bash -c 'echo > /dev/tcp/127.0.0.1/${BROKER_PORT}'" 2>/dev/null; then
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
echo "Next: ./run_kafka_wordcount.sh -input <file> [-output kafka_result.txt]"
