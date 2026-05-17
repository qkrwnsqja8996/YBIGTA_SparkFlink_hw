#!/usr/bin/env bash
# =============================================================================
# Scenario 01: Flink 기본 스트리밍 (실시간 처리 지연시간 측정)
# YBIGTA Spark & Flink HA Homework
# =============================================================================
# Learning: Flink는 이벤트가 Kafka에 도착하는 즉시 처리합니다.
#           processing_latency_ms는 "마지막 이벤트 주입 완료"부터
#           "Flink 출력 파일에 결과가 나타날 때까지"의 시간을 측정합니다.
#           이 값이 작을수록 Flink가 실시간으로 빠르게 처리하고 있다는 의미입니다.
#
# Flow:
#   1. Kafka 오프셋 초기화
#   2. Flink 카운터 잡 시작
#   3. 1000개 이벤트 주입 (주입 완료 시각 기록)
#   4. Flink 처리 완료 대기 (처리 완료 시각 기록)
#   5. 잡 취소
#   6. 출력 해시 검증 + latency/throughput 계산
#
# Pass condition: output counts match expected sha256
#
# Output JSON:
#   {
#     "id": "01",
#     "name": "flink-basic",
#     "passed": true|false,
#     "job_id": "...",
#     "events_injected": 1000,
#     "processing_latency_ms": N,
#     "events_per_second": N,
#     "output_sha256": "...",
#     "expected_sha256": "...",
#     "elapsed_ms": N
#   }
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

source "${PROJECT_ROOT}/scripts/lib/common.sh"
source "${PROJECT_ROOT}/scripts/lib/compose.sh"

SCENARIO_ID="01"
SCENARIO_NAME="flink-basic"
EVENT_COUNT=1000
TOPIC="events"
OUTPUT_FILE="/data/flink-output/counts.txt"
FLINK_WAIT_TIMEOUT=60

START_MS=$(now_ms)
JOB_ID=""
PASSED=false
INJECTION_DONE_MS=0
PROCESSING_DONE_MS=0

# ---------------------------------------------------------------------------
# Cleanup trap
# ---------------------------------------------------------------------------
cleanup() {
    if [[ -n "$JOB_ID" ]]; then
        log_info "Cleanup: cancelling Flink job $JOB_ID"
        flink_cancel_job "$JOB_ID" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Scenario run_timeout check
# ---------------------------------------------------------------------------
check_scenario_timeout() {
    local elapsed=$(( $(now_ms) - START_MS ))
    if [[ $elapsed -gt $(( SCENARIO_TIMEOUT * 1000 )) ]]; then
        log_error "Scenario run_timeout (${SCENARIO_TIMEOUT}s) exceeded"
        output_result false "timeout"
        exit 1
    fi
}

output_result() {
    local passed="$1"
    local note="${2:-}"
    local elapsed=$(( $(now_ms) - START_MS ))

    local actual_sha256="N/A"
    if compose exec -T flink-jm test -f "$OUTPUT_FILE" 2>/dev/null; then
        actual_sha256=$(compose exec -T flink-jm \
            python3 -c "
import hashlib, sys
with open('${OUTPUT_FILE}') as f:
    lines = sorted([l.strip() for l in f if l.strip()])
out = '\n'.join(lines) + '\n'
print(hashlib.sha256(out.encode()).hexdigest())
" 2>/dev/null || echo "ERROR")
    fi

    printf '{"id":"%s","name":"%s","passed":%s,"job_id":"%s","events_injected":%d,"output_sha256":"%s","expected_sha256":"%s","elapsed_ms":%d}\n' \
        "$SCENARIO_ID" \
        "$SCENARIO_NAME" \
        "$passed" \
        "${JOB_ID:-}" \
        "$EVENT_COUNT" \
        "${actual_sha256}" \
        "${EXPECTED_SHA256:-unknown}" \
        "$elapsed"
}

# ---------------------------------------------------------------------------
# Step 1: Compute expected output
# ---------------------------------------------------------------------------
log_info "=== Scenario $SCENARIO_ID: $SCENARIO_NAME ==="
log_info "[Flink 실시간 스트리밍] processing_latency_ms 및 events_per_second 측정"
log_info "Step 1: Computing expected output for $EVENT_COUNT events..."

EXPECTED_SHA256=$(python3 - "$EVENT_COUNT" <<'PYEOF'
import sys, hashlib
n = int(sys.argv[1])
num_types = 5
counts = {f"event_type_{i}": n // num_types for i in range(num_types)}
remainder = n % num_types
for i in range(remainder):
    counts[f"event_type_{i}"] += 1
lines = [f"{et}\t{counts[et]}" for et in sorted(counts.keys())]
output = "\n".join(lines) + "\n"
print(hashlib.sha256(output.encode()).hexdigest())
PYEOF
)
log_info "Expected sha256: $EXPECTED_SHA256"

# ---------------------------------------------------------------------------
# Step 2: Clear previous output and reset topic
# ---------------------------------------------------------------------------
log_info "Step 2: Clearing previous output..."
compose exec -T flink-jm bash -c "rm -f ${OUTPUT_FILE} && mkdir -p /data/flink-output" 2>/dev/null || true

# Reset consumer group offset to latest (so we only see new events)
compose exec -T kafka \
    kafka-consumer-groups \
    --bootstrap-server localhost:29092 \
    --group flink-counter-group \
    --topic "$TOPIC" \
    --reset-offsets \
    --to-latest \
    --execute \
    >/dev/null 2>&1 || true

check_scenario_timeout

# ---------------------------------------------------------------------------
# Step 3: Start Flink counter job
# ---------------------------------------------------------------------------
log_info "Step 3: Starting Flink counter job..."
compose exec -d flink-jm \
    flink run -py /jobs/flink_counter.py \
    >/dev/null 2>&1 || {
    log_error "Failed to submit Flink job"
    output_result false "job_submit_failed"
    exit 1
}

# Wait for job to reach RUNNING state
JOB_ID=$(wait_for_flink_job "$FLINK_WAIT_TIMEOUT" 2) || {
    log_error "Flink job did not start"
    output_result false "job_not_started"
    exit 1
}
log_info "Flink job running: $JOB_ID"

check_scenario_timeout

# ---------------------------------------------------------------------------
# Step 4: Inject events
# ---------------------------------------------------------------------------
log_info "Step 4: Injecting $EVENT_COUNT events..."
INJECT_OUT=$(bash "${PROJECT_ROOT}/scripts/inject_data.sh" \
    --scenario "$SCENARIO_ID" \
    --count "$EVENT_COUNT" \
    --topic "$TOPIC" 2>/dev/null)
INJECTION_DONE_MS=$(now_ms)
log_info "Injection result: $INJECT_OUT"
log_info "이벤트 주입 완료 시각 기록 → latency 측정 시작"

check_scenario_timeout

# ---------------------------------------------------------------------------
# Step 5: Wait for processing
# ---------------------------------------------------------------------------
log_info "Step 5: Waiting ${FLINK_WAIT_TIMEOUT}s for Flink to process events..."
WAIT_DEADLINE=$(( $(now_ms) + FLINK_WAIT_TIMEOUT * 1000 ))

while [[ $(now_ms) -lt $WAIT_DEADLINE ]]; do
    # Check if output file has all expected event types
    if compose exec -T flink-jm test -f "$OUTPUT_FILE" 2>/dev/null; then
        LINE_COUNT=$(compose exec -T flink-jm \
            wc -l < "$OUTPUT_FILE" 2>/dev/null | tr -d ' \r' || echo "0")
        log_info "Output file has $LINE_COUNT lines (need 5)"
        if [[ "$LINE_COUNT" -ge 5 ]]; then
            # Check total count
            TOTAL_COUNT=$(compose exec -T flink-jm \
                python3 -c "
total = 0
with open('${OUTPUT_FILE}') as f:
    for line in f:
        parts = line.strip().split('\t')
        if len(parts) == 2:
            try:
                total += int(parts[1])
            except ValueError:
                pass
print(total)
" 2>/dev/null || echo "0")
            log_info "Total events processed: $TOTAL_COUNT / $EVENT_COUNT"
            if [[ "$TOTAL_COUNT" -ge "$EVENT_COUNT" ]]; then
                PROCESSING_DONE_MS=$(now_ms)
                break
            fi
        fi
    fi
    check_scenario_timeout
    sleep 3
done

# ---------------------------------------------------------------------------
# Step 6: Cancel job
# ---------------------------------------------------------------------------
log_info "Step 6: Cancelling Flink job $JOB_ID..."
flink_cancel_job "$JOB_ID" >/dev/null 2>&1 || true
JOB_ID=""  # Prevent double-cancel in trap
sleep 2

# ---------------------------------------------------------------------------
# Step 7: Verify output
# ---------------------------------------------------------------------------
log_info "Step 7: Verifying output..."

ACTUAL_SHA256=$(compose exec -T flink-jm \
    python3 -c "
import hashlib
try:
    with open('${OUTPUT_FILE}') as f:
        lines = sorted([l.strip() for l in f if l.strip()])
    out = '\n'.join(lines) + '\n'
    print(hashlib.sha256(out.encode()).hexdigest())
except Exception as e:
    print('ERROR: ' + str(e))
" 2>/dev/null || echo "FILE_NOT_FOUND")

log_info "Actual sha256:   $ACTUAL_SHA256"
log_info "Expected sha256: $EXPECTED_SHA256"

ELAPSED_MS=$(( $(now_ms) - START_MS ))

# 지연시간(latency) 및 처리량(throughput) 계산
PROCESSING_LATENCY_MS=0
EVENTS_PER_SEC=0
if [[ $INJECTION_DONE_MS -gt 0 && $PROCESSING_DONE_MS -gt $INJECTION_DONE_MS ]]; then
    PROCESSING_LATENCY_MS=$(( PROCESSING_DONE_MS - INJECTION_DONE_MS ))
    EVENTS_PER_SEC=$(python3 -c "print(round(${EVENT_COUNT} / (${PROCESSING_LATENCY_MS} / 1000), 1))" 2>/dev/null || echo "0")
fi

log_info "처리 지연시간(latency): ${PROCESSING_LATENCY_MS}ms"
log_info "처리량(throughput): ${EVENTS_PER_SEC} events/sec"

if [[ "$ACTUAL_SHA256" == "$EXPECTED_SHA256" ]]; then
    PASSED=true
    log_info "PASSED: 출력 해시 일치! latency=${PROCESSING_LATENCY_MS}ms, throughput=${EVENTS_PER_SEC} events/sec"
else
    PASSED=false
    log_error "FAILED: 출력 해시 불일치"
fi

printf '{"id":"%s","name":"%s","passed":%s,"job_id":"","events_injected":%d,"processing_latency_ms":%d,"events_per_second":%s,"output_sha256":"%s","expected_sha256":"%s","elapsed_ms":%d}\n' \
    "$SCENARIO_ID" \
    "$SCENARIO_NAME" \
    "$PASSED" \
    "$EVENT_COUNT" \
    "$PROCESSING_LATENCY_MS" \
    "$EVENTS_PER_SEC" \
    "$ACTUAL_SHA256" \
    "$EXPECTED_SHA256" \
    "$ELAPSED_MS"
