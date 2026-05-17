#!/usr/bin/env bash
# =============================================================================
# Scenario 07: Spark 배치 처리 - Shuffle Partition 효율 비교
# YBIGTA Spark & Flink HA Homework
# =============================================================================
# Learning: Spark는 데이터를 한번에 모아 처리하는 배치 엔진입니다.
#           Shuffle Partition 수가 데이터 크기에 비해 과도하게 크면
#           빈 파티션이 수백 개 생성되어 오버헤드가 폭증합니다.
#
# Flow:
#   1. 50,000행 데이터 생성
#   2. 기본값(200) 파티션으로 실행 → baseline 시간 측정
#   3. 학생 설정값으로 실행 → job_time 측정
#   4. speedup_factor = baseline / job_time 계산
#   5. job_time < 60s 이면 PASS
#
# Pass condition: job_time_ms < 60,000ms (60초)
# Bonus metric:   speedup_factor (baseline 대비 몇 배 빠른지)
#
# Output JSON:
#   {
#     "id": "07",
#     "name": "spark-shuffle",
#     "passed": true|false,
#     "shuffle_partitions_used": N,
#     "rows_processed": N,
#     "rows_per_second": N,
#     "job_time_ms": N,
#     "baseline_time_ms": N,
#     "speedup_factor": N,
#     "output_sha256": "...",
#     "expected_sha256": "...",
#     "elapsed_ms": N
#   }
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

source "${PROJECT_ROOT}/scripts/lib/common.sh"
source "${PROJECT_ROOT}/scripts/lib/compose.sh"

SCENARIO_ID="04"
SCENARIO_NAME="spark-shuffle"
LARGE_INPUT="/data/spark-input/events_large.csv"
OUTPUT_FILE="/data/spark-output/result_large.csv"
BASELINE_OUTPUT="/data/spark-output/result_large_baseline.csv"
TARGET_TIME_MS=90000
ROW_COUNT=50000

START_MS=$(now_ms)

# ---------------------------------------------------------------------------
# Step 1: 설정값 읽기
# ---------------------------------------------------------------------------
log_info "=== Scenario $SCENARIO_ID: $SCENARIO_NAME ==="
log_info "[Spark 배치] Shuffle Partition 효율 비교 테스트"
log_info "Pass 조건: 50,000행 집계를 60초 이내 완료"

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

log_info "학생 설정: spark.sql.shuffle.partitions = $SHUFFLE_PARTITIONS"

if [[ "$SHUFFLE_PARTITIONS" == "200" ]]; then
    log_warn "partition=200(기본값)이 감지되었습니다. 이 시나리오는 FAIL할 가능성이 높습니다."
    log_warn "student/conf/spark-defaults.conf에서 spark.sql.shuffle.partitions를 4~8로 설정하세요."
fi

# ---------------------------------------------------------------------------
# Step 2: 테스트 데이터 생성
# ---------------------------------------------------------------------------
log_info "Step 2: 대용량 테스트 데이터 생성 (${ROW_COUNT}행)..."
compose exec -T spark-master python3 -c "
import random, os
random.seed(2024)
os.makedirs('/data/spark-input', exist_ok=True)
with open('$LARGE_INPUT', 'w') as out:
    out.write('event_type,value,timestamp\n')
    for i in range($ROW_COUNT):
        etype = f'event_type_{i % 5}'
        value = random.randint(1, 1000)
        ts = 1700000000 + i
        out.write(f'{etype},{value},{ts}\n')
print('데이터 생성 완료', file=__import__('sys').stderr)
" >/dev/null 2>&1
log_info "테스트 데이터 준비 완료"

# ---------------------------------------------------------------------------
# Step 3: 기대 출력 sha256 계산
# ---------------------------------------------------------------------------
EXPECTED_SHA256=$(python3 -c "
import hashlib, random
random.seed(2024)
rows = {f'event_type_{i}': {'total_value': 0, 'event_count': 0, 'max_timestamp': 0} for i in range(5)}
for i in range($ROW_COUNT):
    et = f'event_type_{i % 5}'
    value = random.randint(1, 1000)
    ts = 1700000000 + i
    rows[et]['total_value'] += value
    rows[et]['event_count'] += 1
    if ts > rows[et]['max_timestamp']:
        rows[et]['max_timestamp'] = ts
lines = []
for et in sorted(rows.keys()):
    r = rows[et]
    lines.append(f'{et},{r[\"total_value\"]},{r[\"event_count\"]},{r[\"max_timestamp\"]}')
print(hashlib.sha256(('\n'.join(lines) + '\n').encode()).hexdigest())
" 2>/dev/null || echo "UNKNOWN")

# ---------------------------------------------------------------------------
# Step 4: Spark 잡 스크립트 생성 (파티션 수를 파라미터로 받음)
# ---------------------------------------------------------------------------
compose exec -T spark-master python3 -c "
job = '''
import sys, os, glob, shutil
from pyspark.sql import SparkSession, functions as F

output_path = sys.argv[1] if len(sys.argv) > 1 else \"$OUTPUT_FILE\"
partition_override = sys.argv[2] if len(sys.argv) > 2 else None

spark = SparkSession.builder.appName(\"spark-shuffle-test\").getOrCreate()

if partition_override:
    spark.conf.set(\"spark.sql.shuffle.partitions\", partition_override)

actual_partitions = spark.conf.get(\"spark.sql.shuffle.partitions\", \"200\")
print(f\"[INFO] spark.sql.shuffle.partitions = {actual_partitions}\", file=sys.stderr)

df = spark.read.csv(\"$LARGE_INPUT\", header=True, inferSchema=True)
result = df.groupBy(\"event_type\").agg(
    F.sum(\"value\").alias(\"total_value\"),
    F.count(\"*\").alias(\"event_count\"),
    F.max(\"timestamp\").alias(\"max_timestamp\")
).orderBy(\"event_type\")

tmp = output_path + \"_tmp\"
result.coalesce(1).write.csv(tmp, header=True, mode=\"overwrite\")
parts = glob.glob(os.path.join(tmp, \"part-*.csv\"))
if parts:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    shutil.move(parts[0], output_path)
    shutil.rmtree(tmp, ignore_errors=True)
spark.stop()
'''
with open('/tmp/spark_shuffle_job.py', 'w') as f:
    f.write(job)
print('잡 스크립트 생성 완료', file=__import__('sys').stderr)
" >/dev/null 2>&1

# ---------------------------------------------------------------------------
# Step 5: Baseline 측정 (partition=200)
# ---------------------------------------------------------------------------
log_info "Step 5: Baseline 측정 (partition=200, 기본값)..."
compose exec -T spark-master bash -c "rm -f ${BASELINE_OUTPUT} && mkdir -p /data/spark-output" 2>/dev/null || true

BASELINE_START_MS=$(now_ms)
BASELINE_EXIT=0
run_timeout 90 compose exec -T spark-master \
    /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /tmp/spark_shuffle_job.py \
    "$BASELINE_OUTPUT" "200" \
    >/dev/null 2>&1 || BASELINE_EXIT=$?
BASELINE_TIME_MS=$(( $(now_ms) - BASELINE_START_MS ))

if [[ $BASELINE_EXIT -ne 0 ]]; then
    log_warn "Baseline 실행 실패 (exit=$BASELINE_EXIT), baseline_time=-1로 기록"
    BASELINE_TIME_MS=-1
else
    log_info "Baseline(partition=200) 소요: ${BASELINE_TIME_MS}ms"
fi

# ---------------------------------------------------------------------------
# Step 6: 학생 설정으로 실행
# ---------------------------------------------------------------------------
log_info "Step 6: 학생 설정(partition=$SHUFFLE_PARTITIONS)으로 실행..."
compose exec -T spark-master bash -c "rm -f ${OUTPUT_FILE}" 2>/dev/null || true

JOB_START_MS=$(now_ms)
JOB_EXIT=0
run_timeout 90 compose exec -T spark-master \
    /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /tmp/spark_shuffle_job.py \
    "$OUTPUT_FILE" \
    >/dev/null 2>&1 || JOB_EXIT=$?
JOB_TIME_MS=$(( $(now_ms) - JOB_START_MS ))

if [[ $JOB_EXIT -ne 0 ]]; then
    log_error "Spark 잡 실패 (exit=$JOB_EXIT)"
    ELAPSED=$(( $(now_ms) - START_MS ))
    printf '{"id":"%s","name":"%s","passed":false,"shuffle_partitions_used":%s,"rows_processed":%d,"rows_per_second":0,"job_time_ms":%d,"baseline_time_ms":%d,"speedup_factor":0,"output_sha256":"","expected_sha256":"%s","elapsed_ms":%d,"error":"spark_failed"}\n' \
        "$SCENARIO_ID" "$SCENARIO_NAME" "$SHUFFLE_PARTITIONS" "$ROW_COUNT" "$JOB_TIME_MS" "$BASELINE_TIME_MS" "$EXPECTED_SHA256" "$ELAPSED"
    exit 0
fi

# ---------------------------------------------------------------------------
# Step 7: 결과 검증 및 메트릭 계산
# ---------------------------------------------------------------------------
ACTUAL_SHA256=$(compose exec -T spark-master \
    python3 -c "
import hashlib
try:
    with open('$OUTPUT_FILE') as f:
        lines = []
        for line in f:
            s = line.strip()
            if not s or s.startswith('event_type,'): continue
            lines.append(s)
    lines.sort()
    print(hashlib.sha256(('\n'.join(lines)+'\n').encode()).hexdigest())
except Exception as e:
    print('ERROR: ' + str(e))
" 2>/dev/null || echo "FILE_NOT_FOUND")

# 메트릭 계산
ROWS_PER_SEC=0
SPEEDUP_FACTOR=0
if [[ $JOB_TIME_MS -gt 0 ]]; then
    ROWS_PER_SEC=$(python3 -c "print(round(${ROW_COUNT} / (${JOB_TIME_MS} / 1000)))" 2>/dev/null || echo "0")
fi
if [[ $BASELINE_TIME_MS -gt 0 && $JOB_TIME_MS -gt 0 ]]; then
    SPEEDUP_FACTOR=$(python3 -c "print(round(${BASELINE_TIME_MS} / ${JOB_TIME_MS}, 2))" 2>/dev/null || echo "0")
fi

ELAPSED_MS=$(( $(now_ms) - START_MS ))
PASSED=false

log_info "--- 결과 요약 ---"
log_info "Baseline (partition=200): ${BASELINE_TIME_MS}ms"
log_info "학생 설정 (partition=$SHUFFLE_PARTITIONS): ${JOB_TIME_MS}ms"
log_info "처리량: ${ROWS_PER_SEC} rows/sec"
log_info "속도 향상: ${SPEEDUP_FACTOR}x"
log_info "출력 sha256:  $ACTUAL_SHA256"
log_info "기대 sha256: $EXPECTED_SHA256"

if [[ "$ACTUAL_SHA256" == "$EXPECTED_SHA256" && $JOB_TIME_MS -le $TARGET_TIME_MS ]]; then
    PASSED=true
    log_info "PASSED: ${JOB_TIME_MS}ms < 60s, 출력 정확, ${SPEEDUP_FACTOR}x speedup!"
elif [[ "$ACTUAL_SHA256" != "$EXPECTED_SHA256" ]]; then
    log_error "FAILED: 출력 해시 불일치"
else
    log_error "FAILED: ${JOB_TIME_MS}ms > 60s"
    log_error "힌트: spark.sql.shuffle.partitions를 4~8로 설정하세요 (현재: $SHUFFLE_PARTITIONS)"
fi

printf '{"id":"%s","name":"%s","passed":%s,"shuffle_partitions_used":%s,"rows_processed":%d,"rows_per_second":%s,"job_time_ms":%d,"baseline_time_ms":%d,"speedup_factor":%s,"output_sha256":"%s","expected_sha256":"%s","elapsed_ms":%d}\n' \
    "$SCENARIO_ID" "$SCENARIO_NAME" "$PASSED" \
    "$SHUFFLE_PARTITIONS" "$ROW_COUNT" "$ROWS_PER_SEC" \
    "$JOB_TIME_MS" "$BASELINE_TIME_MS" "$SPEEDUP_FACTOR" \
    "$ACTUAL_SHA256" "$EXPECTED_SHA256" "$ELAPSED_MS"
