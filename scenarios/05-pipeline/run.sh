#!/usr/bin/env bash
# =============================================================================
# Scenario 08: End-to-End Kafka → Flink → Spark Pipeline
# YBIGTA Spark & Flink HA Homework
# =============================================================================
# Learning: End-to-end pipeline from Kafka through Flink to Spark.
#
# Flow:
#   1. Inject 2000 events to Kafka
#   2. Run Flink counter job (reads from Kafka, writes /data/flink-output/counts.txt)
#   3. Wait for Flink to process all events
#   4. Cancel Flink job
#   5. Convert Flink output to CSV for Spark input
#   6. Run Spark aggregation on Flink's output
#   7. Verify final output correctness
#
# Pass condition: end-to-end data flows correctly through both systems
#
# Output JSON:
#   {
#     "id": "08",
#     "name": "pipeline",
#     "passed": true|false,
#     "events_injected": 2000,
#     "flink_output_sha256": "...",
#     "spark_output_sha256": "...",
#     "elapsed_ms": N
#   }
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

source "${PROJECT_ROOT}/scripts/lib/common.sh"
source "${PROJECT_ROOT}/scripts/lib/compose.sh"

SCENARIO_ID="05"
SCENARIO_NAME="pipeline"
EVENT_COUNT=2000
TOPIC="events"
FLINK_OUTPUT="/data/flink-output/counts.txt"
PIPELINE_CSV="/data/spark-input/pipeline_events.csv"
PIPELINE_OUTPUT="/data/spark-output/pipeline_result.csv"
FLINK_WAIT_TIMEOUT=60
PROCESS_TIMEOUT=90

START_MS=$(now_ms)
JOB_ID=""

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------
cleanup() {
    if [[ -n "$JOB_ID" ]]; then
        flink_cancel_job "$JOB_ID" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Step 1: Compute expected Flink output
# ---------------------------------------------------------------------------
log_info "=== Scenario $SCENARIO_ID: $SCENARIO_NAME ==="
log_info "Step 1: Computing expected outputs..."

EXPECTED_FLINK_SHA256=$(python3 - "$EVENT_COUNT" <<'PYEOF'
import sys, hashlib
n = int(sys.argv[1])
num_types = 5
counts = {f"event_type_{i}": n // num_types for i in range(num_types)}
remainder = n % num_types
for i in range(remainder):
    counts[f"event_type_{i}"] += 1
lines = [f"{et}\t{counts[et]}" for et in sorted(counts.keys())]
print(hashlib.sha256(("\n".join(lines) + "\n").encode()).hexdigest())
PYEOF
)
log_info "Expected Flink sha256: $EXPECTED_FLINK_SHA256"

# ---------------------------------------------------------------------------
# Step 2: Reset state
# ---------------------------------------------------------------------------
log_info "Step 2: Resetting state..."
compose exec -T flink-jm bash -c \
    "rm -f ${FLINK_OUTPUT} && mkdir -p /data/flink-output /data/checkpoints" 2>/dev/null || true
compose exec -T spark-master bash -c \
    "rm -f ${PIPELINE_OUTPUT} && mkdir -p /data/spark-output" 2>/dev/null || true

compose exec -T kafka \
    kafka-consumer-groups \
    --bootstrap-server localhost:29092 \
    --group flink-counter-group \
    --topic "$TOPIC" \
    --reset-offsets --to-latest --execute \
    >/dev/null 2>&1 || true

# ---------------------------------------------------------------------------
# Step 3: Start Flink job
# ---------------------------------------------------------------------------
log_info "Step 3: Starting Flink counter job..."
# Cancel any lingering Flink jobs from previous scenarios
flink_cancel_all >/dev/null 2>&1 || true
sleep 2
compose exec -d flink-jm \
    flink run -py /jobs/flink_counter.py \
    >/dev/null 2>&1

JOB_ID=$(wait_for_flink_job "$FLINK_WAIT_TIMEOUT" 2) || {
    log_error "Flink job did not start"
    ELAPSED=$(( $(now_ms) - START_MS ))
    printf '{"id":"%s","name":"%s","passed":false,"events_injected":%d,"flink_output_sha256":"","spark_output_sha256":"","elapsed_ms":%d,"error":"flink_not_started"}\n' \
        "$SCENARIO_ID" "$SCENARIO_NAME" "$EVENT_COUNT" "$ELAPSED"
    exit 0
}
log_info "Flink job running: $JOB_ID"

# ---------------------------------------------------------------------------
# Step 4: Inject events
# ---------------------------------------------------------------------------
log_info "Step 4: Injecting $EVENT_COUNT events to Kafka..."
bash "${PROJECT_ROOT}/scripts/inject_data.sh" \
    --scenario "$SCENARIO_ID" \
    --count "$EVENT_COUNT" \
    --topic "$TOPIC" >/dev/null 2>&1
log_info "Events injected"

# ---------------------------------------------------------------------------
# Step 5: Wait for Flink to process all events
# ---------------------------------------------------------------------------
log_info "Step 5: Waiting for Flink to process $EVENT_COUNT events..."
DEADLINE=$(( $(now_ms) + PROCESS_TIMEOUT * 1000 ))
ALL_PROCESSED=false

while [[ $(now_ms) -lt $DEADLINE ]]; do
    if compose exec -T flink-jm test -f "$FLINK_OUTPUT" 2>/dev/null; then
        TOTAL=$(compose exec -T flink-jm python3 -c "
total=0
try:
    with open('${FLINK_OUTPUT}') as f:
        for line in f:
            p=line.strip().split('\t')
            if len(p)==2:
                try: total+=int(p[1])
                except: pass
except: pass
print(total)
" 2>/dev/null || echo "0")
        log_info "Events processed by Flink: $TOTAL / $EVENT_COUNT"
        if [[ "$TOTAL" -ge "$EVENT_COUNT" ]]; then
            ALL_PROCESSED=true
            break
        fi
    fi
    sleep 5
done

# Cancel Flink job
log_info "Cancelling Flink job $JOB_ID..."
flink_cancel_job "$JOB_ID" >/dev/null 2>&1 || true
JOB_ID=""
sleep 2

if [[ "$ALL_PROCESSED" != "true" ]]; then
    log_error "Flink did not process all events within ${PROCESS_TIMEOUT}s"
    ELAPSED=$(( $(now_ms) - START_MS ))
    printf '{"id":"%s","name":"%s","passed":false,"events_injected":%d,"flink_output_sha256":"","spark_output_sha256":"","elapsed_ms":%d,"error":"flink_timeout"}\n' \
        "$SCENARIO_ID" "$SCENARIO_NAME" "$EVENT_COUNT" "$ELAPSED"
    exit 0
fi

# Verify Flink output
FLINK_SHA256=$(compose exec -T flink-jm \
    python3 -c "
import hashlib
with open('${FLINK_OUTPUT}') as f:
    lines = sorted([l.strip() for l in f if l.strip()])
print(hashlib.sha256(('\n'.join(lines)+'\n').encode()).hexdigest())
" 2>/dev/null || echo "ERROR")
log_info "Flink output sha256: $FLINK_SHA256"

# ---------------------------------------------------------------------------
# Step 6: Convert Flink output to CSV for Spark
# ---------------------------------------------------------------------------
log_info "Step 6: Converting Flink output to Spark input CSV..."
compose exec -T spark-master bash -c "
python3 -c \"
import os
flink_out = '${FLINK_OUTPUT}'
spark_in = '${PIPELINE_CSV}'
os.makedirs(os.path.dirname(spark_in), exist_ok=True)
with open(flink_out) as fin, open(spark_in, 'w') as fout:
    fout.write('event_type,value,timestamp\n')
    for line in fin:
        parts = line.strip().split('\t')
        if len(parts) == 2:
            et, count = parts[0], int(parts[1])
            # Use count as value, 0 as timestamp placeholder
            fout.write(f'{et},{count},0\n')
print('Converted Flink output to Spark input', file=__import__('sys').stderr)
\"" >/dev/null 2>&1
log_info "Conversion complete"

# ---------------------------------------------------------------------------
# Step 7: Run Spark on pipeline data
# ---------------------------------------------------------------------------
log_info "Step 7: Running Spark aggregation on Flink output..."

# Create job that reads pipeline_events.csv
compose exec -T spark-master python3 -c "
job = '''
import sys, os, glob, shutil
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.appName('spark-pipeline').getOrCreate()
df = spark.read.csv('$PIPELINE_CSV', header=True, inferSchema=True)
result = df.groupBy('event_type').agg(
    F.sum('value').alias('total_value'),
    F.count('*').alias('event_count'),
    F.max('timestamp').alias('max_timestamp')
).orderBy('event_type')
tmp = '$PIPELINE_OUTPUT' + '_tmp'
result.coalesce(1).write.csv(tmp, header=True, mode='overwrite')
parts = glob.glob(os.path.join(tmp, 'part-*.csv'))
if parts:
    os.makedirs(os.path.dirname('$PIPELINE_OUTPUT'), exist_ok=True)
    shutil.move(parts[0], '$PIPELINE_OUTPUT')
    shutil.rmtree(tmp, ignore_errors=True)
spark.stop()
'''
with open('/tmp/spark_pipeline_job.py', 'w') as f:
    f.write(job)
" 2>/dev/null

SPARK_EXIT=0
run_timeout 90 compose exec -T spark-master \
    /opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /tmp/spark_pipeline_job.py \
    >/dev/null 2>&1 || SPARK_EXIT=$?

if [[ $SPARK_EXIT -ne 0 ]]; then
    log_error "Spark pipeline job failed"
    ELAPSED=$(( $(now_ms) - START_MS ))
    printf '{"id":"%s","name":"%s","passed":false,"events_injected":%d,"flink_output_sha256":"%s","spark_output_sha256":"","elapsed_ms":%d,"error":"spark_failed"}\n' \
        "$SCENARIO_ID" "$SCENARIO_NAME" "$EVENT_COUNT" "$FLINK_SHA256" "$ELAPSED"
    exit 0
fi

# ---------------------------------------------------------------------------
# Step 8: Verify Spark output
# ---------------------------------------------------------------------------
SPARK_SHA256=$(compose exec -T spark-master \
    python3 -c "
import hashlib
try:
    with open('$PIPELINE_OUTPUT') as f:
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

ELAPSED_MS=$(( $(now_ms) - START_MS ))
PASSED=false

log_info "Flink sha256: $FLINK_SHA256 (expected: $EXPECTED_FLINK_SHA256)"
log_info "Spark output sha256: $SPARK_SHA256"

if [[ "$FLINK_SHA256" == "$EXPECTED_FLINK_SHA256" && ! "$SPARK_SHA256" =~ ^ERROR ]]; then
    PASSED=true
    log_info "PASSED: End-to-end pipeline completed successfully!"
else
    log_error "FAILED: Pipeline produced incorrect results"
    log_error "  Flink match: $([ "$FLINK_SHA256" = "$EXPECTED_FLINK_SHA256" ] && echo true || echo false)"
    log_error "  Spark error: $([[ "$SPARK_SHA256" =~ ^ERROR ]] && echo true || echo false)"
fi

printf '{"id":"%s","name":"%s","passed":%s,"events_injected":%d,"flink_output_sha256":"%s","spark_output_sha256":"%s","elapsed_ms":%d}\n' \
    "$SCENARIO_ID" "$SCENARIO_NAME" "$PASSED" \
    "$EVENT_COUNT" "$FLINK_SHA256" "$SPARK_SHA256" "$ELAPSED_MS"
