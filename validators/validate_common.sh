#!/usr/bin/env bash
# =============================================================================
# Cluster Validation Script
# YBIGTA Spark & Flink HA Homework
# =============================================================================
# Runs common cluster health checks and outputs JSON.
#
# Usage:
#   ./validators/validate_common.sh
#
# Output JSON:
#   {
#     "passed": true|false,
#     "flink_healthy": true|false,
#     "spark_healthy": true|false,
#     "no_failed_jobs": true|false,
#     "flink_taskmanagers": N,
#     "spark_workers": N
#   }
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------

FLINK_OK=false
SPARK_OK=false
NO_FAILED_JOBS_OK=false
FLINK_TM_COUNT=0
SPARK_WORKER_COUNT=0

if flink_cluster_healthy 2; then
    FLINK_OK=true
    FLINK_TM_COUNT=$(curl -sf "http://localhost:8081/overview" \
        | python3 -c "import sys,json; print(json.load(sys.stdin).get('taskmanagers',0))" \
        2>/dev/null || echo "0")
fi

if spark_cluster_healthy 2; then
    SPARK_OK=true
    SPARK_WORKER_COUNT=$(curl -sf "http://localhost:8080/json" \
        | python3 -c "
import sys, json
d = json.load(sys.stdin)
alive = [w for w in d.get('workers',[]) if w.get('state')=='ALIVE']
print(len(alive))
" 2>/dev/null || echo "0")
fi

if check_flink_no_failed_jobs; then
    NO_FAILED_JOBS_OK=true
fi

PASSED=false
if [[ "$FLINK_OK" == "true" && "$SPARK_OK" == "true" && "$NO_FAILED_JOBS_OK" == "true" ]]; then
    PASSED=true
fi

# ---------------------------------------------------------------------------
# Output JSON
# ---------------------------------------------------------------------------
printf '{"passed":%s,"flink_healthy":%s,"spark_healthy":%s,"no_failed_jobs":%s,"flink_taskmanagers":%d,"spark_workers":%d}\n' \
    "$PASSED" \
    "$FLINK_OK" \
    "$SPARK_OK" \
    "$NO_FAILED_JOBS_OK" \
    "$FLINK_TM_COUNT" \
    "$SPARK_WORKER_COUNT"
