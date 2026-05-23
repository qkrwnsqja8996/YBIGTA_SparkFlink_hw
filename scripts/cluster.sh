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
    log_info "GHCR에서 base image pull 중... (최초 1회는 시간이 좀 걸림)"
    compose pull
    log_info "Pull complete"
}

cmd_init() {
    printf '\n' >&2
    printf '================================================================\n' >&2
    printf '  YBIGTA Spark & Flink 클러스터 시작\n' >&2
    printf '  지금부터 아래 컴포넌트들이 Docker 컨테이너로 올라옵니다.\n' >&2
    printf '================================================================\n\n' >&2

    # Start ZooKeeper first
    printf '  [1/4] Kafka 브로커 시작\n' >&2
    printf '        ZooKeeper  : Kafka 브로커 코디네이터\n' >&2
    printf '        Kafka      : 이벤트 스트림 저장소 — Flink/Spark가 여기서 데이터를 읽어갑니다\n\n' >&2
    compose up -d --force-recreate zookeeper
    wait_for_service zookeeper 120
    compose up -d --force-recreate kafka
    wait_for_service kafka 120
    create_kafka_topic events 4 1

    # Start Flink
    printf '\n  [2/4] Flink 클러스터 시작\n' >&2
    printf '        JobManager  × 1 : 잡 스케줄링, 태스크 분배, 체크포인트 조율\n' >&2
    printf '        TaskManager × 3 : 실제 스트림 연산 수행 (슬롯 단위로 병렬 처리)\n\n' >&2
    compose up -d --force-recreate flink-jm
    wait_for_service flink-jm 120
    compose up -d --force-recreate flink-tm1 flink-tm2 flink-tm3

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

    fix_flink_data_permissions

    # Start Spark
    printf '\n  [3/4] Spark 클러스터 시작\n' >&2
    printf '        Master × 1 : Driver — DAG 생성, 태스크 분배\n' >&2
    printf '        Worker × 3 : Executor — 실제 배치 연산 수행\n\n' >&2
    compose up -d --force-recreate spark-master
    wait_for_service spark-master 120
    compose up -d --force-recreate spark-worker1 spark-worker2 spark-worker3

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
    printf '\n  [4/4] 테스트 데이터 로드\n' >&2
    printf '        시나리오에서 사용할 CSV / 이벤트 데이터를 생성합니다\n\n' >&2
    load_spark_test_data

    # Initialize results directory
    mkdir -p "${PROJECT_ROOT}/results"
    if [[ ! -f "${PROJECT_ROOT}/results/result.json" ]]; then
        printf '{}' > "${PROJECT_ROOT}/results/result.json"
    fi

    printf '\n================================================================\n' >&2
    printf '  클러스터 준비 완료!\n' >&2
    printf '\n' >&2
    printf '  Flink UI  →  http://localhost:8081  (JobManager, TaskManager 상태)\n' >&2
    printf '  Spark UI  →  http://localhost:8080  (Master, Worker 상태)\n' >&2
    printf '================================================================\n\n' >&2
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
    SPARK_HTML=$(curl -sf "http://localhost:8080/" 2>/dev/null || echo "")
    if echo "$SPARK_HTML" | grep -q "Spark Master at"; then
        # 워커 수는 "Alive Workers" 옆 숫자에서 파싱
        WORKER_COUNT=$(echo "$SPARK_HTML" | grep -oE "Alive Workers:?</[^>]*>[[:space:]]*[0-9]+" | grep -oE "[0-9]+" | head -1)
        WORKER_COUNT="${WORKER_COUNT:-?}"
        printf '  Spark Master: running\n  Workers alive: %s\n' "$WORKER_COUNT"
    else
        log_warn "Spark UI not accessible at http://localhost:8080" >&2
    fi
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
