#!/usr/bin/env bash
# macOS/Linux 호환 timeout 래퍼
run_timeout() {
    local secs="$1"; shift
    if command -v gtimeout &>/dev/null; then
        gtimeout "$secs" "$@"
    elif command -v timeout &>/dev/null; then
        timeout "$secs" "$@"
    else
        # timeout 없으면 그냥 실행 (macOS 기본)
        "$@"
    fi
}
# =============================================================================
# Run All Scenarios
# YBIGTA Spark & Flink HA Homework
# =============================================================================
# Runs all 9 scenarios sequentially, collects results, and writes
# results/result.json with a summary for grading.
#
# Usage:
#   ./run_all.sh                    # Run all scenarios
#   ./run_all.sh --scenarios 1,2,3  # Run specific scenarios
#   ./run_all.sh --timeout 180      # Set per-scenario timeout (default: 120)
#
# Output: results/result.json
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/scripts/lib/common.sh"
source "${SCRIPT_DIR}/scripts/lib/compose.sh"

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
RESULTS_DIR="${SCRIPT_DIR}/results"
RESULT_FILE="${RESULTS_DIR}/result.json"
TMP_DIR="${RESULTS_DIR}/tmp_$$"
export SCENARIO_TIMEOUT="${SCENARIO_TIMEOUT:-120}"
RUN_SCENARIOS=""

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --scenarios|-s)  RUN_SCENARIOS="$2";       shift 2 ;;
        --timeout|-t)
            SCENARIO_TIMEOUT="$2"
            export SCENARIO_TIMEOUT
            shift 2
            ;;
        --help|-h)
            printf 'Usage: %s [--scenarios 1,2,3] [--timeout N]\n' "$0"
            exit 0
            ;;
        *) log_error "Unknown argument: $1"; exit 1 ;;
    esac
done

# ---------------------------------------------------------------------------
# Cleanup tmp dir on exit
# ---------------------------------------------------------------------------
cleanup_tmp() {
    rm -rf "$TMP_DIR"
}
trap cleanup_tmp EXIT
mkdir -p "$TMP_DIR"

# ---------------------------------------------------------------------------
# Scenario definitions (ordered)
# ---------------------------------------------------------------------------
SCENARIO_IDS=(01 02 03 04 05)

# bash 3.x 호환: 연관 배열 대신 함수로 조회
scenario_dir() {
    case "$1" in
        01) echo "${SCRIPT_DIR}/scenarios/01-flink-basic" ;;
        02) echo "${SCRIPT_DIR}/scenarios/02-spark-batch" ;;
        03) echo "${SCRIPT_DIR}/scenarios/03-stream-vs-batch" ;;
        04) echo "${SCRIPT_DIR}/scenarios/04-spark-shuffle" ;;
        05) echo "${SCRIPT_DIR}/scenarios/05-pipeline" ;;
    esac
}
scenario_name() {
    case "$1" in
        01) echo "flink-basic" ;;
        02) echo "spark-batch" ;;
        03) echo "stream-vs-batch" ;;
        04) echo "spark-shuffle" ;;
        05) echo "pipeline" ;;
    esac
}

# Filter to requested scenarios if specified
if [[ -n "$RUN_SCENARIOS" ]]; then
    FILTERED=()
    IFS=',' read -ra REQUESTED <<< "$RUN_SCENARIOS"
    for sid in "${SCENARIO_IDS[@]}"; do
        for req in "${REQUESTED[@]}"; do
            req_norm=$(printf '%02d' "$((10#$req))" 2>/dev/null || echo "$req")
            if [[ "$sid" == "$req_norm" || "$sid" == "$req" ]]; then
                FILTERED+=("$sid")
                break
            fi
        done
    done
    SCENARIO_IDS=("${FILTERED[@]}")
fi

# ---------------------------------------------------------------------------
# Pre-flight check
# ---------------------------------------------------------------------------
log_info "============================================================"
log_info "YBIGTA Spark & Flink Homework - Run All Scenarios"
log_info "============================================================"
log_info "Scenarios to run: ${SCENARIO_IDS[*]}"
log_info "Per-scenario timeout: ${SCENARIO_TIMEOUT}s"
log_info "Results file: $RESULT_FILE"
printf '\n' >&2

log_info "Pre-flight: checking cluster health..."
CLUSTER_OK=true
if ! curl -sf "http://localhost:8081/overview" >/dev/null 2>&1; then
    log_error "Flink JobManager not accessible at http://localhost:8081"
    CLUSTER_OK=false
fi
if ! curl -sf "http://localhost:8080/json" >/dev/null 2>&1; then
    log_error "Spark Master not accessible at http://localhost:8080"
    CLUSTER_OK=false
fi
if [[ "$CLUSTER_OK" != "true" ]]; then
    log_error "Cluster is not running. Run: ./scripts/cluster.sh init"
    exit 1
fi
log_info "Cluster is accessible."

# ---------------------------------------------------------------------------
# Reset state before scenarios — prevents stale checkpoints and accumulated
# Kafka events from causing hash mismatches on second+ runs.
# ---------------------------------------------------------------------------
log_info "Resetting state from previous runs..."

# Clear Flink checkpoints and outputs
compose exec -T flink-jm bash -c \
    "rm -rf /data/checkpoints/* /data/flink-output/* 2>/dev/null; \
     mkdir -p /data/checkpoints /data/flink-output" 2>/dev/null || true

# Delete and recreate Kafka 'events' topic so it starts empty
compose exec -T kafka bash -c "
    kafka-topics --bootstrap-server localhost:29092 --delete --topic events 2>/dev/null || true
    sleep 2
    kafka-topics --bootstrap-server localhost:29092 --create --topic events \
        --partitions 4 --replication-factor 1 --if-not-exists 2>/dev/null || true
" 2>/dev/null || true

# Delete Flink consumer group so it doesn't restart from old offsets
compose exec -T kafka kafka-consumer-groups \
    --bootstrap-server localhost:29092 \
    --delete --group flink-counter-group 2>/dev/null || true

printf '\n' >&2

# ---------------------------------------------------------------------------
# Run each scenario
# ---------------------------------------------------------------------------
mkdir -p "$RESULTS_DIR"
TOTAL_START_MS=$(now_ms)
SCENARIOS_PASSED=0
SCENARIOS_TOTAL=${#SCENARIO_IDS[@]}
TOTAL_PENALTY_MS=0

for scenario_id in "${SCENARIO_IDS[@]}"; do
    scenario_dir="$(scenario_dir "$scenario_id")"
    scenario_name="$(scenario_name "$scenario_id")"
    run_script="${scenario_dir}/run.sh"
    scenario_json_file="${TMP_DIR}/scenario_${scenario_id}.json"

    log_info "------------------------------------------------------------"
    log_info "Running scenario $scenario_id: $scenario_name"
    log_info "Timeout: ${SCENARIO_TIMEOUT}s"
    log_info "------------------------------------------------------------"

    if [[ ! -f "$run_script" ]]; then
        log_error "Script not found: $run_script"
        printf '{"id":"%s","name":"%s","passed":false,"error":"script_not_found","elapsed_ms":0}\n' \
            "$scenario_id" "$scenario_name" > "$scenario_json_file"
        continue
    fi

    chmod +x "$run_script"

    SCENARIO_START_MS=$(now_ms)
    EXIT_CODE=0

    # Run with timeout; stderr → terminal, stdout → JSON file
    run_timeout "${SCENARIO_TIMEOUT}" bash "$run_script" \
        >"$scenario_json_file" \
        2> >(tee /dev/stderr >/dev/null) \
        || EXIT_CODE=$?

    SCENARIO_ELAPSED=$(( $(now_ms) - SCENARIO_START_MS ))

    # Handle timeout or failure
    if [[ $EXIT_CODE -eq 124 ]]; then
        log_error "Scenario $scenario_id TIMED OUT after ${SCENARIO_TIMEOUT}s"
        printf '{"id":"%s","name":"%s","passed":false,"error":"timeout","elapsed_ms":%d}\n' \
            "$scenario_id" "$scenario_name" "$SCENARIO_ELAPSED" > "$scenario_json_file"
    elif [[ ! -s "$scenario_json_file" ]]; then
        log_error "Scenario $scenario_id produced no output (exit: $EXIT_CODE)"
        printf '{"id":"%s","name":"%s","passed":false,"error":"no_output","exit_code":%d,"elapsed_ms":%d}\n' \
            "$scenario_id" "$scenario_name" "$EXIT_CODE" "$SCENARIO_ELAPSED" > "$scenario_json_file"
    fi

    # Read result
    SCENARIO_JSON=$(cat "$scenario_json_file" 2>/dev/null || echo '{}')

    PASSED=$(python3 -c "
import json, sys
try:
    d = json.loads(sys.stdin.read())
    print('true' if d.get('passed', False) else 'false')
except Exception:
    print('false')
" <<< "$SCENARIO_JSON" 2>/dev/null || echo "false")

    ELAPSED=$(python3 -c "
import json, sys
try:
    d = json.loads(sys.stdin.read())
    print(d.get('elapsed_ms', 0))
except Exception:
    print(0)
" <<< "$SCENARIO_JSON" 2>/dev/null || echo "0")

    # Save individual result file
    cp "$scenario_json_file" "${RESULTS_DIR}/scenario_${scenario_id}.json"

    if [[ "$PASSED" == "true" ]]; then
        SCENARIOS_PASSED=$(( SCENARIOS_PASSED + 1 ))
        log_info "RESULT: PASSED (${ELAPSED}ms)"
    else
        TOTAL_PENALTY_MS=$(( TOTAL_PENALTY_MS + ELAPSED ))
        log_error "RESULT: FAILED (penalty: ${ELAPSED}ms)"
    fi

    printf '\n' >&2
    sleep 2
done

# ---------------------------------------------------------------------------
# Write final result.json
# ---------------------------------------------------------------------------
TOTAL_ELAPSED_MS=$(( $(now_ms) - TOTAL_START_MS ))

# Collect all scenario JSONs into array
python3 - "$RESULTS_DIR" "${SCENARIO_IDS[@]}" <<PYEOF
import json, sys, os

results_dir = sys.argv[1]
ids = sys.argv[2:]

scenarios = []
for sid in ids:
    path = os.path.join(results_dir, f'scenario_{sid}.json')
    try:
        with open(path) as f:
            scenarios.append(json.load(f))
    except Exception as e:
        scenarios.append({"id": sid, "error": str(e), "passed": False})

result = {
    "scenarios_passed": $SCENARIOS_PASSED,
    "scenarios_total": $SCENARIOS_TOTAL,
    "penalty_ms": $TOTAL_PENALTY_MS,
    "total_elapsed_ms": $TOTAL_ELAPSED_MS,
    "scenarios": scenarios,
}

with open("${RESULT_FILE}", "w") as f:
    json.dump(result, f, indent=2)

print("Results written to ${RESULT_FILE}", file=sys.stderr)
PYEOF

# ---------------------------------------------------------------------------
# Print summary
# ---------------------------------------------------------------------------
log_info "============================================================"
log_info "ALL SCENARIOS COMPLETE"
log_info "  Passed: $SCENARIOS_PASSED / $SCENARIOS_TOTAL"
log_info "  Penalty ms: $TOTAL_PENALTY_MS"
log_info "  Total time: ${TOTAL_ELAPSED_MS}ms"
log_info "============================================================"

python3 - "${RESULT_FILE}" <<'PYEOF'
import json, sys
with open(sys.argv[1]) as f:
    d = json.load(f)
for s in d.get("scenarios", []):
    status = "PASS" if s.get("passed") else "FAIL"
    sid = s.get("id", "?")
    name = s.get("name", "?")
    ms = s.get("elapsed_ms", 0)
    print(f"  [{status}] {sid} - {name} ({ms}ms)", file=sys.stderr)
PYEOF

printf '\n' >&2
log_info "Full results saved to: $RESULT_FILE"
