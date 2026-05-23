#!/usr/bin/env bash
# =============================================================================
# Scenario 04: Spark 기본 배치 집계 (배치 처리 메트릭 측정)
# YBIGTA Spark & Flink HA Homework
# =============================================================================
# Learning: Spark는 CSV 전체를 메모리에 올려 한번에 집계하는 배치 엔진입니다.
#           rows_per_second는 Spark가 처리한 배치 처리량을 나타냅니다.
#           shuffle_partitions_used를 통해 설정이 올바른지 확인할 수 있습니다.
#
# Flow:
#   1. 테스트 데이터(10,000행) 존재 확인
#   2. spark_aggregate.py 제출 및 시간 측정
#   3. 출력 sha256 검증
#   4. rows_per_second, shuffle_partitions_used 계산
#
# Pass condition: output sha256 matches expected
#
# Output JSON:
#   {
#     "id": "04",
#     "name": "spark-basic",
#     "passed": true|false,
#     "rows_processed": N,
#     "rows_per_second": N,
#     "shuffle_partitions_used": N,
#     "job_time_ms": N,
#     "output_sha256": "...",
#     "expected_sha256": "...",
#     "elapsed_ms": N
#   }
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

source "${PROJECT_ROOT}/scripts/lib/common.sh"
source "${PROJECT_ROOT}/scripts/lib/compose.sh"

SCENARIO_ID="02"
SCENARIO_NAME="spark-batch"
INPUT_FILE="/data/spark-input/events.csv"
OUTPUT_FILE="/data/spark-output/result.csv"
SPARK_TIMEOUT=120

START_MS=$(now_ms)

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
cleanup() {
    : # No streaming jobs to cancel
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Step 1: Compute expected sha256 from the input data
# ---------------------------------------------------------------------------
log_info "=== Scenario $SCENARIO_ID: $SCENARIO_NAME ==="
log_info "[Spark 배치] rows_per_second 및 shuffle_partitions_used 측정"
log_info "Step 1: Computing expected output sha256..."

EXPECTED_SHA256=$(compose exec -T spark-master \
    python3 -c "
import hashlib, csv
rows = {}
try:
    with open('${INPUT_FILE}') as f:
        reader = csv.DictReader(f)
        for row in reader:
            et = row['event_type']
            val = int(row['value'])
            rows[et] = rows.get(et, {'total_value': 0, 'event_count': 0, 'max_timestamp': 0})
            rows[et]['total_value'] += val
            rows[et]['event_count'] += 1
            ts = int(row['timestamp'])
            if ts > rows[et]['max_timestamp']:
                rows[et]['max_timestamp'] = ts
    lines = []
    for et in sorted(rows.keys()):
        r = rows[et]
        lines.append(f'{et},{r[\"total_value\"]},{r[\"event_count\"]},{r[\"max_timestamp\"]}')
    output = '\n'.join(lines) + '\n'
    print(hashlib.sha256(output.encode()).hexdigest())
except Exception as e:
    print('ERROR: ' + str(e))
" 2>/dev/null || echo "ERROR")

log_info "Expected sha256: $EXPECTED_SHA256"

if [[ "$EXPECTED_SHA256" == ERROR* ]]; then
    log_error "Failed to compute expected sha256. Is test data loaded?"
    log_error "Run: ./scripts/cluster.sh init"
    ELAPSED=$(( $(now_ms) - START_MS ))
    printf '{"id":"%s","name":"%s","passed":false,"output_sha256":"","expected_sha256":"","elapsed_ms":%d,"error":"no_test_data"}\n' \
        "$SCENARIO_ID" "$SCENARIO_NAME" "$ELAPSED"
    exit 0
fi

# ---------------------------------------------------------------------------
# Step 2: Clear previous output
# ---------------------------------------------------------------------------
log_info "Step 2: Clearing previous output..."
compose exec -T spark-master bash -c "rm -f ${OUTPUT_FILE} && mkdir -p /data/spark-output" 2>/dev/null || true

# ---------------------------------------------------------------------------
# Step 3: Submit Spark job
# ---------------------------------------------------------------------------
log_info "Step 3: Submitting spark_aggregate.py (timeout: ${SPARK_TIMEOUT}s)..."

JOB_START_MS=$(now_ms)
JOB_EXIT=0
run_timeout "$SPARK_TIMEOUT" compose exec -T spark-master \
    /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /jobs/spark_aggregate.py \
    2>&1 | grep -v "^$" | while IFS= read -r line; do
        log_info "[spark] $line"
    done || JOB_EXIT=$?
JOB_TIME_MS=$(( $(now_ms) - JOB_START_MS ))

if [[ $JOB_EXIT -ne 0 ]]; then
    log_error "Spark job failed with exit code $JOB_EXIT"
    ELAPSED=$(( $(now_ms) - START_MS ))
    printf '{"id":"%s","name":"%s","passed":false,"output_sha256":"","expected_sha256":"%s","elapsed_ms":%d,"error":"spark_job_failed"}\n' \
        "$SCENARIO_ID" "$SCENARIO_NAME" "$EXPECTED_SHA256" "$ELAPSED"
    exit 0
fi

log_info "Spark job completed"

# ---------------------------------------------------------------------------
# Step 4: Verify output
# ---------------------------------------------------------------------------
log_info "Step 4: Verifying output..."

ACTUAL_SHA256=$(compose exec -T spark-master \
    python3 -c "
import hashlib
try:
    with open('${OUTPUT_FILE}') as f:
        # Skip header if present
        lines = []
        for line in f:
            stripped = line.strip()
            if not stripped: continue
            if stripped.startswith('event_type,'): continue  # header
            lines.append(stripped)
    lines.sort()
    output = '\n'.join(lines) + '\n'
    print(hashlib.sha256(output.encode()).hexdigest())
except Exception as e:
    print('ERROR: ' + str(e))
" 2>/dev/null || echo "FILE_NOT_FOUND")

ELAPSED_MS=$(( $(now_ms) - START_MS ))
PASSED=false

# 배치 메트릭 계산
ROW_COUNT=10000
ROWS_PER_SEC=0
if [[ $JOB_TIME_MS -gt 0 ]]; then
    ROWS_PER_SEC=$(python3 -c "print(round(${ROW_COUNT} / (${JOB_TIME_MS} / 1000)))" 2>/dev/null || echo "0")
fi

SHUFFLE_PARTITIONS=$(compose exec -T spark-master \
    python3 -c "
try:
    with open('/opt/bitnami/spark/conf/spark-defaults.conf') as f:
        for line in f:
            s = line.strip()
            if s.startswith('#'): continue
            if 'spark.sql.shuffle.partitions' in s:
                parts = s.split()
                if len(parts) >= 2:
                    print(parts[-1])
                    exit(0)
    print('200')
except Exception:
    print('200')
" 2>/dev/null || echo "200")

log_info "Actual sha256:   $ACTUAL_SHA256"
log_info "Expected sha256: $EXPECTED_SHA256"
log_info "배치 처리량: ${ROWS_PER_SEC} rows/sec (${JOB_TIME_MS}ms for ${ROW_COUNT} rows)"
log_info "shuffle.partitions: $SHUFFLE_PARTITIONS"

if [[ "$ACTUAL_SHA256" == "$EXPECTED_SHA256" ]]; then
    PASSED=true
    log_info "PASSED: 출력 해시 일치! throughput=${ROWS_PER_SEC} rows/sec"
else
    log_error "FAILED: 출력 해시 불일치"
fi

printf '{"id":"%s","name":"%s","passed":%s,"rows_processed":%d,"rows_per_second":%s,"shuffle_partitions_used":%s,"job_time_ms":%d,"output_sha256":"%s","expected_sha256":"%s","elapsed_ms":%d}\n' \
    "$SCENARIO_ID" "$SCENARIO_NAME" "$PASSED" \
    "$ROW_COUNT" "$ROWS_PER_SEC" "$SHUFFLE_PARTITIONS" "$JOB_TIME_MS" \
    "$ACTUAL_SHA256" "$EXPECTED_SHA256" "$ELAPSED_MS"
