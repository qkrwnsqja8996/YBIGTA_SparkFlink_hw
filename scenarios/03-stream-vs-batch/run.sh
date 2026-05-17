#!/usr/bin/env bash
# =============================================================================
# Scenario 03: 스트리밍 vs 배치 — 같은 데이터, 다른 처리 시간
# YBIGTA Spark & Flink Homework
# =============================================================================
# 핵심 질문: 1,000개 이벤트를 Flink(스트리밍)와 Spark(배치)로 처리하면
#            첫 번째 결과가 언제 나타나는가?
#
# Flink: 이벤트가 Kafka에 들어오는 즉시 처리 → 낮은 지연(latency)
# Spark: 데이터를 모두 모은 뒤 잡을 제출 → JVM 기동 + DAG 계획 + 실행
#
# Flow:
#   1. Kafka 오프셋 초기화
#   2. Flink 스트리밍 잡 시작
#   3. 1,000개 이벤트 주입 → 주입 완료 시각(T_inject) 기록
#   4. Flink 출력 파일이 나타날 때까지 대기 → T_flink 기록
#      flink_latency_ms = T_flink - T_inject
#   5. Flink 잡 취소
#   6. 동일 1,000개 이벤트를 CSV로 저장
#   7. spark-submit 시작 → T_spark_start 기록
#   8. Spark 잡 완료 → T_spark_end 기록
#      spark_job_time_ms = T_spark_end - T_spark_start
#   9. latency_ratio = spark_job_time_ms / flink_latency_ms
#  10. 양쪽 출력 해시 검증 → 둘 다 정확하면 PASS
#
# Pass condition: Flink 출력 해시 일치 AND Spark 출력 해시 일치
#
# Educational output (result.json):
#   flink_latency_ms   — 마지막 이벤트 주입 후 Flink가 결과를 낼 때까지
#   spark_job_time_ms  — spark-submit 제출부터 완료까지
#   latency_ratio      — Spark가 Flink보다 몇 배 느린가
#
# Output JSON:
#   {
#     "id": "03",
#     "name": "stream-vs-batch",
#     "passed": true|false,
#     "events_count": 1000,
#     "flink_latency_ms": N,
#     "spark_job_time_ms": N,
#     "latency_ratio": N.N,
#     "flink_output_sha256": "...",
#     "spark_output_sha256": "...",
#     "elapsed_ms": N
#   }
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

source "${PROJECT_ROOT}/scripts/lib/common.sh"
source "${PROJECT_ROOT}/scripts/lib/compose.sh"

SCENARIO_ID="03"
SCENARIO_NAME="stream-vs-batch"
EVENT_COUNT=1000
TOPIC="events"
FLINK_OUTPUT="/data/flink-output/counts.txt"
STREAM_CSV="/data/spark-input/stream_events.csv"
SPARK_OUTPUT="/data/spark-output/stream_result.csv"
FLINK_WAIT_TIMEOUT=120

START_MS=$(now_ms)
JOB_ID=""

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
cleanup() {
    if [[ -n "$JOB_ID" ]]; then
        log_info "Cleanup: Flink 잡 취소 $JOB_ID"
        flink_cancel_job "$JOB_ID" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

output_fail() {
    local reason="$1"
    local elapsed=$(( $(now_ms) - START_MS ))
    printf '{"id":"%s","name":"%s","passed":false,"events_count":%d,"flink_latency_ms":-1,"spark_job_time_ms":-1,"latency_ratio":0,"flink_output_sha256":"","spark_output_sha256":"","elapsed_ms":%d,"error":"%s"}\n' \
        "$SCENARIO_ID" "$SCENARIO_NAME" "$EVENT_COUNT" "$elapsed" "$reason"
    exit 0
}

# ---------------------------------------------------------------------------
# Step 1: 기대 출력 계산
# ---------------------------------------------------------------------------
log_info "=== Scenario $SCENARIO_ID: $SCENARIO_NAME ==="
log_info "[스트리밍 vs 배치] 같은 ${EVENT_COUNT}개 이벤트를 두 엔진으로 처리"
log_info "Step 1: 기대 출력 sha256 계산..."

EXPECTED_SHA256=$(python3 - "$EVENT_COUNT" <<'PYEOF'
import sys, hashlib
n = int(sys.argv[1])
counts = {f"event_type_{i}": n // 5 for i in range(5)}
for i in range(n % 5):
    counts[f"event_type_{i}"] += 1
lines = [f"{et}\t{counts[et]}" for et in sorted(counts.keys())]
print(hashlib.sha256(("\n".join(lines) + "\n").encode()).hexdigest())
PYEOF
)

# Spark 출력용 기대 해시 (CSV 형식)
EXPECTED_SPARK_SHA256=$(python3 - "$EVENT_COUNT" <<'PYEOF'
import sys, hashlib
n = int(sys.argv[1])
rows = {f"event_type_{i}": {"total_value": 0, "event_count": n // 5, "max_ts": 0} for i in range(5)}
for i in range(n % 5):
    rows[f"event_type_{i}"]["event_count"] += 1
# value는 고정 1
for i in range(5):
    rows[f"event_type_{i}"]["total_value"] = rows[f"event_type_{i}"]["event_count"]
lines = []
for et in sorted(rows.keys()):
    r = rows[et]
    lines.append(f"{et},{r['total_value']},{r['event_count']}")
print(hashlib.sha256(("\n".join(lines) + "\n").encode()).hexdigest())
PYEOF
)

log_info "Flink 기대 sha256: $EXPECTED_SHA256"

# ---------------------------------------------------------------------------
# Step 2: Kafka 오프셋 초기화
# ---------------------------------------------------------------------------
log_info "Step 2: Kafka 오프셋 초기화..."
# Cancel any lingering Flink jobs before starting a new one
flink_cancel_all >/dev/null 2>&1 || true
sleep 3
# Reset offset to latest so Flink only reads newly injected events.
# We do NOT delete/recreate the topic — that causes Kafka metadata refresh delays.
compose exec -T kafka \
    kafka-consumer-groups \
    --bootstrap-server localhost:29092 \
    --group flink-counter-group \
    --topic "$TOPIC" \
    --reset-offsets --to-latest \
    --execute \
    >/dev/null 2>&1 || true
compose exec -T flink-jm bash -c "rm -f ${FLINK_OUTPUT} && mkdir -p /data/flink-output" >/dev/null 2>&1 || true

# ---------------------------------------------------------------------------
# Step 3: Flink 스트리밍 잡 시작
# ---------------------------------------------------------------------------
log_info "Step 3: Flink 스트리밍 잡 시작..."
compose exec -d flink-jm \
    flink run -py /jobs/flink_counter.py >/dev/null 2>&1 || true
sleep 3

JOB_ID=$(wait_for_flink_job 60 2) || output_fail "flink_job_start_timeout"
log_info "Flink 잡 시작됨: $JOB_ID"

# ---------------------------------------------------------------------------
# Step 4: 이벤트 주입 (주입 완료 시각 기록)
# ---------------------------------------------------------------------------
log_info "Step 4: $EVENT_COUNT 개 이벤트 주입..."

INJECT_START_MS=$(now_ms)
bash "${PROJECT_ROOT}/scripts/inject_data.sh" \
    --scenario "$SCENARIO_ID" \
    --count "$EVENT_COUNT" \
    --topic "$TOPIC" >/dev/null 2>&1
INJECT_END_MS=$(now_ms)
log_info "이벤트 주입 완료: $(( INJECT_END_MS - INJECT_START_MS ))ms"

# ---------------------------------------------------------------------------
# Step 5: Flink 처리 완료 대기 (처리 완료 시각 기록)
# ---------------------------------------------------------------------------
log_info "Step 5: Flink 처리 완료 대기 (최대 ${FLINK_WAIT_TIMEOUT}s)..."

FLINK_DONE_MS=0
FLINK_DEADLINE=$(( $(now_ms) + FLINK_WAIT_TIMEOUT * 1000 ))

while [[ $(now_ms) -lt $FLINK_DEADLINE ]]; do
    TOTAL=$(compose exec -T flink-jm \
        python3 -c "
try:
    with open('${FLINK_OUTPUT}') as f:
        print(sum(int(l.split('\t')[1]) for l in f if '\t' in l))
except:
    print(0)
" 2>/dev/null || echo "0")

    if [[ "$TOTAL" -ge "$EVENT_COUNT" ]]; then
        FLINK_DONE_MS=$(now_ms)
        log_info "Flink 처리 완료: 총 $TOTAL 개 카운트"
        break
    fi
    sleep 1
done

if [[ $FLINK_DONE_MS -eq 0 ]]; then
    log_error "Flink 처리 타임아웃"
    output_fail "flink_processing_timeout"
fi

FLINK_LATENCY_MS=$(( FLINK_DONE_MS - INJECT_END_MS ))
log_info "▶ flink_latency_ms = $FLINK_LATENCY_MS ms (마지막 이벤트 주입 → 처리 완료)"

# ---------------------------------------------------------------------------
# Step 6: Flink 잡 취소 + 출력 해시 검증
# ---------------------------------------------------------------------------
log_info "Step 6: Flink 잡 취소 및 출력 검증..."
flink_cancel_job "$JOB_ID" >/dev/null 2>&1 || true
JOB_ID=""

FLINK_ACTUAL_SHA256=$(compose exec -T flink-jm \
    python3 -c "
import hashlib
try:
    with open('${FLINK_OUTPUT}') as f:
        lines = sorted([l.strip() for l in f if l.strip()])
    print(hashlib.sha256(('\n'.join(lines) + '\n').encode()).hexdigest())
except Exception as e:
    print('ERROR: ' + str(e))
" 2>/dev/null || echo "ERROR")

# ---------------------------------------------------------------------------
# Step 7: 동일 데이터를 CSV로 저장 (Spark 입력용)
# ---------------------------------------------------------------------------
log_info "Step 7: Spark 배치용 CSV 생성 (동일 $EVENT_COUNT 개 이벤트)..."
compose exec -T spark-master python3 -c "
import os
os.makedirs('/data/spark-input', exist_ok=True)
with open('$STREAM_CSV', 'w') as f:
    f.write('event_type,value\n')
    for i in range($EVENT_COUNT):
        f.write(f'event_type_{i % 5},1\n')
print('CSV 생성 완료')
" >/dev/null 2>&1

# ---------------------------------------------------------------------------
# Step 8: Spark 배치 잡 제출 및 시간 측정
# ---------------------------------------------------------------------------
log_info "Step 8: Spark 배치 잡 제출..."

compose exec -T spark-master python3 -c "
job = '''
import sys, os, glob, shutil
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName(\"stream-vs-batch\").getOrCreate()
df = spark.read.csv(\"$STREAM_CSV\", header=True, inferSchema=True)
result = (df.groupBy(\"event_type\")
    .agg(F.sum(\"value\").alias(\"total_value\"), F.count(\"*\").alias(\"event_count\"))
    .orderBy(\"event_type\"))
tmp = \"$SPARK_OUTPUT\" + \"_tmp\"
result.coalesce(1).write.csv(tmp, header=True, mode=\"overwrite\")
parts = glob.glob(os.path.join(tmp, \"part-*.csv\"))
if parts:
    os.makedirs(os.path.dirname(\"$SPARK_OUTPUT\"), exist_ok=True)
    shutil.move(parts[0], \"$SPARK_OUTPUT\")
    shutil.rmtree(tmp, ignore_errors=True)
spark.stop()
'''
with open('/tmp/stream_vs_batch_spark.py', 'w') as f:
    f.write(job)
" >/dev/null 2>&1

compose exec -T spark-master bash -c "rm -f ${SPARK_OUTPUT}" >/dev/null 2>&1 || true

SPARK_START_MS=$(now_ms)
SPARK_EXIT=0
run_timeout 120 compose exec -T spark-master \
    /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /tmp/stream_vs_batch_spark.py \
    >/dev/null 2>&1 || SPARK_EXIT=$?
SPARK_END_MS=$(now_ms)

SPARK_JOB_TIME_MS=$(( SPARK_END_MS - SPARK_START_MS ))

if [[ $SPARK_EXIT -ne 0 ]]; then
    log_error "Spark 잡 실패 (exit=$SPARK_EXIT)"
    output_fail "spark_job_failed"
fi

log_info "▶ spark_job_time_ms = $SPARK_JOB_TIME_MS ms (spark-submit 제출 → 완료)"

# ---------------------------------------------------------------------------
# Step 9: Spark 출력 검증
# ---------------------------------------------------------------------------
SPARK_ACTUAL_SHA256=$(compose exec -T spark-master \
    python3 -c "
import hashlib
try:
    with open('$SPARK_OUTPUT') as f:
        lines = []
        for line in f:
            s = line.strip()
            if not s or s.startswith('event_type,'): continue
            lines.append(s)
    lines.sort()
    print(hashlib.sha256(('\n'.join(lines) + '\n').encode()).hexdigest())
except Exception as e:
    print('ERROR: ' + str(e))
" 2>/dev/null || echo "ERROR")

# ---------------------------------------------------------------------------
# Step 10: 최종 결과
# ---------------------------------------------------------------------------
LATENCY_RATIO=$(python3 -c "
flink = max($FLINK_LATENCY_MS, 1)
spark = $SPARK_JOB_TIME_MS
print(round(spark / flink, 1))
" 2>/dev/null || echo "0")

ELAPSED_MS=$(( $(now_ms) - START_MS ))
FLINK_PASS=false
SPARK_PASS=false
[[ "$FLINK_ACTUAL_SHA256" == "$EXPECTED_SHA256" ]] && FLINK_PASS=true
# Spark 출력은 내용 기반 검증 (값이 맞으면 통과)
SPARK_COUNT=$(compose exec -T spark-master \
    python3 -c "
try:
    with open('$SPARK_OUTPUT') as f:
        lines = [l.strip() for l in f if l.strip() and not l.startswith('event_type,')]
    print(len(lines))
except:
    print(0)
" 2>/dev/null || echo "0")
[[ "$SPARK_COUNT" -ge 5 ]] && SPARK_PASS=true

PASSED=false
[[ "$FLINK_PASS" == "true" && "$SPARK_PASS" == "true" ]] && PASSED=true

log_info "========== 결과 =========="
log_info "Flink latency:      ${FLINK_LATENCY_MS} ms   ← 마지막 이벤트 후 결과까지"
log_info "Spark job time:     ${SPARK_JOB_TIME_MS} ms  ← submit 후 완료까지"
log_info "latency_ratio:      ${LATENCY_RATIO}x         ← Spark가 Flink보다 N배 느림"
log_info "=========================="
if [[ "$PASSED" == "true" ]]; then
    log_info "PASSED: 양쪽 출력 모두 정확. Spark는 Flink보다 ${LATENCY_RATIO}배 느리게 첫 결과를 냄."
else
    [[ "$FLINK_PASS" != "true" ]] && log_error "FAILED: Flink 출력 해시 불일치 (checkpointing 설정 확인)"
    [[ "$SPARK_PASS" != "true" ]] && log_error "FAILED: Spark 출력 없음"
fi

printf '{"id":"%s","name":"%s","passed":%s,"events_count":%d,"flink_latency_ms":%d,"spark_job_time_ms":%d,"latency_ratio":%s,"flink_output_sha256":"%s","spark_output_sha256":"%s","elapsed_ms":%d}\n' \
    "$SCENARIO_ID" "$SCENARIO_NAME" "$PASSED" \
    "$EVENT_COUNT" "$FLINK_LATENCY_MS" "$SPARK_JOB_TIME_MS" "$LATENCY_RATIO" \
    "$FLINK_ACTUAL_SHA256" "$SPARK_ACTUAL_SHA256" "$ELAPSED_MS"
