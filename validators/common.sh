#!/usr/bin/env bash
# =============================================================================
# Validator Common Functions
# YBIGTA Spark & Flink HA Homework
# =============================================================================

[[ -n "${_SPFL_VALIDATORS_COMMON_SH_LOADED:-}" ]] && return 0
_SPFL_VALIDATORS_COMMON_SH_LOADED=1

# Source common library
_VALIDATORS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${_VALIDATORS_DIR}/../scripts/lib/common.sh"

# ---------------------------------------------------------------------------
# Flink cluster health
# ---------------------------------------------------------------------------

# Check if Flink cluster is healthy
# Returns 0 if healthy, 1 if not
flink_cluster_healthy() {
    local min_taskmanagers="${1:-2}"

    # Check JM is reachable
    if ! curl -sf "http://localhost:8081/overview" >/dev/null 2>&1; then
        log_warn "Flink JobManager not reachable at http://localhost:8081"
        return 1
    fi

    # Check TM count
    local tm_count
    tm_count=$(curl -sf "http://localhost:8081/overview" \
        | python3 -c "import sys,json; print(json.load(sys.stdin).get('taskmanagers',0))" \
        2>/dev/null || echo "0")

    if [[ "$tm_count" -lt "$min_taskmanagers" ]]; then
        log_warn "Flink: only $tm_count TaskManagers connected (need >= $min_taskmanagers)"
        return 1
    fi

    log_info "Flink cluster healthy: $tm_count TaskManagers connected"
    return 0
}

# ---------------------------------------------------------------------------
# Spark cluster health
# ---------------------------------------------------------------------------

# Check if Spark cluster is healthy
# Returns 0 if healthy, 1 if not
spark_cluster_healthy() {
    local min_workers="${1:-2}"

    # Check master is reachable
    if ! curl -sf "http://localhost:8080/json" >/dev/null 2>&1; then
        log_warn "Spark Master not reachable at http://localhost:8080"
        return 1
    fi

    # Check worker count
    local worker_count
    worker_count=$(curl -sf "http://localhost:8080/json" \
        | python3 -c "
import sys, json
d = json.load(sys.stdin)
alive = [w for w in d.get('workers',[]) if w.get('state')=='ALIVE']
print(len(alive))
" 2>/dev/null || echo "0")

    if [[ "$worker_count" -lt "$min_workers" ]]; then
        log_warn "Spark: only $worker_count workers alive (need >= $min_workers)"
        return 1
    fi

    log_info "Spark cluster healthy: $worker_count workers alive"
    return 0
}

# ---------------------------------------------------------------------------
# Flink job state checks
# ---------------------------------------------------------------------------

# Check that no Flink jobs are in FAILED state
# Returns 0 if no failed jobs, 1 if any failed
check_flink_no_failed_jobs() {
    local failed_count
    failed_count=$(curl -sf "http://localhost:8081/jobs/overview" \
        | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    failed = [j for j in data.get('jobs',[]) if j.get('state')=='FAILED']
    print(len(failed))
except Exception:
    print(0)
" 2>/dev/null || echo "0")

    if [[ "$failed_count" -gt 0 ]]; then
        log_warn "Flink: $failed_count job(s) in FAILED state"
        return 1
    fi

    log_info "Flink: no failed jobs"
    return 0
}

# ---------------------------------------------------------------------------
# Output hash verification (delegates to common.sh)
# ---------------------------------------------------------------------------

# verify_output_hash is defined in common.sh:
#   verify_output_hash <output_file> <expected_sha256>
