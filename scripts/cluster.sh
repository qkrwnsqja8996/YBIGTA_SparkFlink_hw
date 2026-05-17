#!/usr/bin/env bash
# =============================================================================
# Cluster Management Script
# YBIGTA Spark & Flink HA Homework
# =============================================================================
# Usage:
#   ./scripts/cluster.sh build   - Build Docker images
#   ./scripts/cluster.sh init    - Start services, create topics, load test data
#   ./scripts/cluster.sh up      - Start existing containers
#   ./scripts/cluster.sh down    - Stop containers (keep volumes)
#   ./scripts/cluster.sh clean   - Stop containers and delete all volumes
#   ./scripts/cluster.sh status  - Show container status and cluster info
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

source "${SCRIPT_DIR}/lib/common.sh"
source "${SCRIPT_DIR}/lib/compose.sh"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

wait_for_service() {
    local service="$1"
    local timeout="${2:-120}"
    local start
    start=$(now_ms)

    log_info "Waiting for $service to be healthy (timeout: ${timeout}s)..."
    while true; do
        local elapsed=$(( ($(now_ms) - start) / 1000 ))
        if [[ $elapsed -gt $timeout ]]; then
            log_error "$service did not become healthy within ${timeout}s"
            return 1
        fi

        local status
        status=$(compose ps --format json "$service" 2>/dev/null \
            | python3 -c "
import sys, json
try:
    for line in sys.stdin:
        line = line.strip()
        if not line: continue
        d = json.loads(line)
        print(d.get('Health', d.get('Status', '')))
        break
except Exception:
    pass
" 2>/dev/null || true)

        if [[ "$status" == "healthy" ]]; then
            log_info "$service is healthy"
            return 0
        fi

        log_info "$service status: ${status:-unknown} (${elapsed}s elapsed)"
        sleep 5
    done
}

create_kafka_topic() {
    local topic="$1"
    local partitions="${2:-4}"
    local replication="${3:-1}"

    log_info "Creating Kafka topic: $topic"
    compose exec -T kafka \
        kafka-topics \
        --bootstrap-server localhost:29092 \
        --create \
        --if-not-exists \
        --topic "$topic" \
        --partitions "$partitions" \
        --replication-factor "$replication" \
        2>/dev/null || log_warn "Topic $topic may already exist"
}

fix_flink_data_permissions() {
    log_info "Fixing /data/checkpoints and /data/flink-output ownership for flink user..."
    compose exec -T flink-jm bash -c "
        mkdir -p /data/checkpoints /data/flink-output
        chown -R flink:flink /data/checkpoints /data/flink-output
    " 2>/dev/null || true

    log_info "Ensuring kafka-clients JAR in all flink containers..."
    local jar_url="https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.2.3/kafka-clients-3.2.3.jar"
    local jar_path="/opt/flink/lib/kafka-clients-3.2.3.jar"
    for svc in flink-jm flink-tm1 flink-tm2 flink-tm3; do
        if ! compose exec -T "$svc" test -f "$jar_path" 2>/dev/null; then
            log_info "  Downloading kafka-clients JAR into $svc..."
            compose exec -T "$svc" curl -fsSL "$jar_url" -o "$jar_path" 2>/dev/null || \
                log_warn "  Failed to download kafka-clients JAR for $svc"
        fi
    done
}

load_spark_test_data() {
    log_info "Loading Spark test data..."
    compose exec -T spark-master bash -c "
        mkdir -p /data/spark-input /data/spark-output /data/flink-output /data/checkpoints

        # Generate test data CSV
        python3 -c \"
import random
random.seed(42)
print('event_type,value,timestamp')
num_types = 5
total = 10000
for i in range(total):
    etype = f'event_type_{i % num_types}'
    value = random.randint(1, 100)
    ts = 1700000000 + i
    print(f'{etype},{value},{ts}')
\" > /data/spark-input/events.csv
        echo 'Test data written: /data/spark-input/events.csv'
        wc -l /data/spark-input/events.csv
    "
    log_info "Spark test data loaded"
}

# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

cmd_build() {
    log_info "Building Docker images..."
    compose build --no-cache
    log_info "Build complete"
}

cmd_init() {
    log_info "Initializing cluster..."

    # Start ZooKeeper first
    log_info "Starting ZooKeeper..."
    compose up -d zookeeper
    wait_for_service zookeeper 120

    # Start Kafka
    log_info "Starting Kafka..."
    compose up -d kafka
    wait_for_service kafka 120

    # Create Kafka topics
    create_kafka_topic events 4 1

    # Start Flink
    log_info "Starting Flink JobManager..."
    compose up -d flink-jm
    wait_for_service flink-jm 120

    log_info "Starting Flink TaskManagers..."
    compose up -d flink-tm1 flink-tm2 flink-tm3

    # Wait for TaskManagers to connect
    log_info "Waiting for TaskManagers to connect..."
    local deadline=$(( $(now_ms) + 60000 ))
    while [[ $(now_ms) -lt $deadline ]]; do
        local tm_count
        tm_count=$(curl -sf "http://localhost:8081/overview" \
            | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('taskmanagers',0))" \
            2>/dev/null || echo "0")
        if [[ "$tm_count" -ge 2 ]]; then
            log_info "TaskManagers connected: $tm_count"
            break
        fi
        log_info "Waiting for TaskManagers... ($tm_count connected)"
        sleep 5
    done

    # Fix flink data permissions and ensure kafka-clients JAR
    fix_flink_data_permissions

    # Start Spark
    log_info "Starting Spark Master..."
    compose up -d spark-master
    wait_for_service spark-master 120

    log_info "Starting Spark Workers..."
    compose up -d spark-worker1 spark-worker2 spark-worker3

    # Wait for Spark workers
    log_info "Waiting for Spark Workers to connect..."
    deadline=$(( $(now_ms) + 60000 ))
    while [[ $(now_ms) -lt $deadline ]]; do
        local worker_count
        worker_count=$(curl -sf "http://localhost:8080/json" \
            | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(d.get('workers',[])))" \
            2>/dev/null || echo "0")
        if [[ "$worker_count" -ge 2 ]]; then
            log_info "Spark workers connected: $worker_count"
            break
        fi
        log_info "Waiting for Spark workers... ($worker_count connected)"
        sleep 5
    done

    # Load test data
    load_spark_test_data

    # Initialize results directory
    mkdir -p "${PROJECT_ROOT}/results"
    if [[ ! -f "${PROJECT_ROOT}/results/result.json" ]]; then
        printf '{}' > "${PROJECT_ROOT}/results/result.json"
    fi

    log_info "Cluster initialization complete!"
    log_info "Flink UI: http://localhost:8081"
    log_info "Spark UI: http://localhost:8080"
    cmd_status
}

cmd_up() {
    log_info "Starting containers..."
    compose up -d
    log_info "Containers started"
    cmd_status
}

cmd_down() {
    log_info "Stopping containers (volumes preserved)..."
    compose down
    log_info "Containers stopped"
}

cmd_clean() {
    log_info "Stopping containers and removing volumes..."
    compose down -v --remove-orphans
    log_info "Cluster cleaned"
}

cmd_status() {
    log_info "Container status:"
    compose ps 2>/dev/null || true
    printf '\n' >&2

    log_info "Flink cluster overview:"
    curl -sf "http://localhost:8081/overview" \
        | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    print(f'  JobManager: running')
    print(f'  TaskManagers: {d.get(\"taskmanagers\", 0)}')
    print(f'  Slots available: {d.get(\"slots-available\", 0)}/{d.get(\"slots-total\", 0)}')
    print(f'  Running jobs: {d.get(\"jobs-running\", 0)}')
except Exception:
    print('  Flink JM not reachable at http://localhost:8081')
" 2>/dev/null || log_warn "Flink UI not accessible" >&2

    printf '\n' >&2
    log_info "Spark cluster overview:"
    curl -sf "http://localhost:8080/json" \
        | python3 -c "
import sys, json
try:
    d = json.load(sys.stdin)
    workers = d.get('workers', [])
    alive = [w for w in workers if w.get('state') == 'ALIVE']
    print(f'  Spark Master: running')
    print(f'  Workers: {len(alive)} alive / {len(workers)} total')
except Exception:
    print('  Spark Master not reachable at http://localhost:8080')
" 2>/dev/null || log_warn "Spark UI not accessible" >&2
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

CMD="${1:-help}"
shift || true

case "$CMD" in
    build)   cmd_build   "$@" ;;
    init)    cmd_init    "$@" ;;
    up)      cmd_up      "$@" ;;
    down)    cmd_down    "$@" ;;
    clean)   cmd_clean   "$@" ;;
    status)  cmd_status  "$@" ;;
    help|--help|-h)
        printf 'Usage: %s {build|init|up|down|clean|status}\n' "$0"
        printf '  build   Build Docker images\n'
        printf '  init    Start services, create topics, load test data\n'
        printf '  up      Start existing containers\n'
        printf '  down    Stop containers (keep volumes)\n'
        printf '  clean   Stop containers and delete all volumes\n'
        printf '  status  Show container and cluster status\n'
        ;;
    *)
        log_error "Unknown command: $CMD"
        printf 'Usage: %s {build|init|up|down|clean|status}\n' "$0"
        exit 1
        ;;
esac
