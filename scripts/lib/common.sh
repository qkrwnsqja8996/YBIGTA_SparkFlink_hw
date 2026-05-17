#!/usr/bin/env bash
# =============================================================================
# Common Library Functions
# YBIGTA Spark & Flink HA Homework
# =============================================================================
# Source this file from scenario/validator scripts:
#   source "$(dirname "$0")/../../scripts/lib/common.sh"

# Prevent double-sourcing
[[ -n "${_SPFL_COMMON_SH_LOADED:-}" ]] && return 0
_SPFL_COMMON_SH_LOADED=1

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

log_info() {
    printf '[INFO]  %s %s\n' "$(date '+%H:%M:%S')" "$*" >&2
}

log_warn() {
    printf '[WARN]  %s %s\n' "$(date '+%H:%M:%S')" "$*" >&2
}

log_error() {
    printf '[ERROR] %s %s\n' "$(date '+%H:%M:%S')" "$*" >&2
}

# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

# Current time in milliseconds
now_ms() {
    if date -r /dev/null +%s%3N 2>/dev/null | grep -q '^[0-9]'; then
        # BSD date (macOS) - use python fallback
        python3 -c 'import time; print(int(time.time() * 1000))'
    else
        date +%s%3N
    fi
}

# Escape a string for JSON
json_escape() {
    local s="$1"
    s="${s//\\/\\\\}"    # backslash
    s="${s//\"/\\\"}"    # double quote
    s="${s//$'\n'/\\n}"  # newline
    s="${s//$'\t'/\\t}"  # tab
    s="${s//$'\r'/\\r}"  # carriage return
    printf '%s' "$s"
}

# ---------------------------------------------------------------------------
# Flink REST API
# ---------------------------------------------------------------------------

FLINK_REST_URL="${FLINK_REST_URL:-http://localhost:8081}"

# Wrapper for Flink REST API calls
flink_api() {
    local method="${1:-GET}"
    local path="${2:-/overview}"
    local body="${3:-}"
    local url="${FLINK_REST_URL}${path}"

    if [[ -n "$body" ]]; then
        curl -sf -X "$method" -H 'Content-Type: application/json' \
             -d "$body" "$url" 2>/dev/null
    else
        curl -sf -X "$method" "$url" 2>/dev/null
    fi
}

# Get list of running job IDs
flink_running_jobs() {
    flink_api GET /jobs/overview 2>/dev/null \
        | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    jobs = data.get('jobs', [])
    running = [j['jid'] for j in jobs if j.get('state') == 'RUNNING']
    print(' '.join(running))
except Exception:
    pass
" 2>/dev/null
}

# Get status of a specific job ID
flink_job_status() {
    local job_id="$1"
    flink_api GET "/jobs/${job_id}" 2>/dev/null \
        | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    print(data.get('state', 'UNKNOWN'))
except Exception:
    print('UNKNOWN')
" 2>/dev/null
}

# Wait for a Flink job to reach RUNNING state
# Args: [timeout_seconds] [poll_interval_seconds]
# Returns: job_id on stdout (first running job found)
wait_for_flink_job() {
    local timeout="${1:-60}"
    local interval="${2:-2}"
    local deadline=$(( $(now_ms) + timeout * 1000 ))

    log_info "Waiting for Flink job to start (timeout: ${timeout}s)..."
    while [[ $(now_ms) -lt $deadline ]]; do
        local jobs
        jobs=$(flink_running_jobs)
        if [[ -n "$jobs" ]]; then
            local job_id
            job_id=$(echo "$jobs" | awk '{print $1}')
            log_info "Flink job is RUNNING: $job_id"
            echo "$job_id"
            return 0
        fi
        sleep "$interval"
    done
    log_error "Timed out waiting for Flink job to start"
    return 1
}

# Wait for a specific Flink job to reach RUNNING state
# Args: job_id [timeout_seconds]
wait_for_flink_job_id() {
    local job_id="$1"
    local timeout="${2:-60}"
    local deadline=$(( $(now_ms) + timeout * 1000 ))

    log_info "Waiting for job $job_id to be RUNNING (timeout: ${timeout}s)..."
    while [[ $(now_ms) -lt $deadline ]]; do
        local status
        status=$(flink_job_status "$job_id")
        if [[ "$status" == "RUNNING" ]]; then
            log_info "Job $job_id is RUNNING"
            return 0
        fi
        log_info "Job $job_id status: $status"
        sleep 2
    done
    log_error "Job $job_id did not reach RUNNING state within ${timeout}s"
    return 1
}

# Cancel a Flink job
flink_cancel_job() {
    local job_id="$1"
    log_info "Cancelling Flink job: $job_id"
    flink_api PATCH "/jobs/${job_id}?mode=cancel" >/dev/null 2>&1 || true
}

# Cancel all running Flink jobs
flink_cancel_all() {
    local jobs
    jobs=$(flink_running_jobs)
    for job_id in $jobs; do
        flink_cancel_job "$job_id"
    done
}

# Get Flink cluster overview (number of taskmanagers, slots etc.)
flink_overview() {
    flink_api GET /overview 2>/dev/null
}

# Get number of connected TaskManagers
flink_taskmanager_count() {
    flink_overview \
        | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    print(data.get('taskmanagers', 0))
except Exception:
    print(0)
" 2>/dev/null
}

# ---------------------------------------------------------------------------
# Spark
# ---------------------------------------------------------------------------

SPARK_SUBMIT="${SPARK_SUBMIT:-/opt/bitnami/spark/bin/spark-submit}"
SPARK_MASTER="${SPARK_MASTER:-spark://spark-master:7077}"

# Submit a Spark job inside the spark-master container and wait for completion
# Args: script_path [extra args...]
# Returns: 0 on success, 1 on failure
spark_submit_and_wait() {
    local script_path="$1"
    shift
    log_info "Submitting Spark job: $script_path"
    docker exec spark-master \
        /opt/bitnami/spark/bin/spark-submit \
        --master "$SPARK_MASTER" \
        "$script_path" \
        "$@"
}

# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka:9092}"

# Produce N events to Kafka topic
# Args: topic count [scenario_id]
# Output: JSON with count and sha256
kafka_produce() {
    local topic="${1:-events}"
    local count="${2:-100}"
    local scenario_id="${3:-0}"

    docker exec -i kafka \
        /usr/bin/kafka-console-producer \
        --bootstrap-server localhost:29092 \
        --topic "$topic"
}

# Compute expected sha256 for N events across 5 types
# Events are distributed as: type_i gets floor(N/5) events, with remainder going to lower types
compute_expected_sha256() {
    local count="$1"
    python3 - "$count" <<'PYEOF'
import sys
import hashlib

n = int(sys.argv[1])
num_types = 5
counts = {}
for i in range(num_types):
    counts[f"event_type_{i}"] = n // num_types

# Distribute remainder
remainder = n % num_types
for i in range(remainder):
    counts[f"event_type_{i}"] += 1

# Sort by event_type and build output string
lines = []
for et in sorted(counts.keys()):
    lines.append(f"{et}\t{counts[et]}")
output = "\n".join(lines) + "\n"
sha = hashlib.sha256(output.encode()).hexdigest()
print(sha)
PYEOF
}

# ---------------------------------------------------------------------------
# Output verification
# ---------------------------------------------------------------------------

# Compute sha256 of the counts output file
compute_output_sha256() {
    local output_file="$1"
    if [[ ! -f "$output_file" ]]; then
        log_warn "Output file not found: $output_file"
        echo "FILE_NOT_FOUND"
        return 1
    fi

    # Read and sort the output file, then compute hash
    python3 - "$output_file" <<'PYEOF'
import sys
import hashlib

path = sys.argv[1]
with open(path, 'r') as f:
    lines = [l.strip() for l in f if l.strip()]

lines.sort()
output = "\n".join(lines) + "\n"
sha = hashlib.sha256(output.encode()).hexdigest()
print(sha)
PYEOF
}

# Verify the output hash matches expected
# Args: output_file expected_sha256
# Returns: 0 if match, 1 if not
verify_output_hash() {
    local output_file="$1"
    local expected_sha256="$2"

    local actual_sha256
    actual_sha256=$(compute_output_sha256 "$output_file")
    local rc=$?

    if [[ $rc -ne 0 ]]; then
        log_error "Failed to compute sha256 of $output_file"
        return 1
    fi

    if [[ "$actual_sha256" == "$expected_sha256" ]]; then
        log_info "Output hash matches: $actual_sha256"
        return 0
    else
        log_error "Output hash mismatch!"
        log_error "  Expected: $expected_sha256"
        log_error "  Actual:   $actual_sha256"
        return 1
    fi
}

# ---------------------------------------------------------------------------
# Scenario timeout helper
# ---------------------------------------------------------------------------

SCENARIO_TIMEOUT="${SCENARIO_TIMEOUT:-120}"

# Check if we've exceeded the scenario timeout
# Args: start_ms
check_timeout() {
    local start_ms="$1"
    local elapsed=$(( $(now_ms) - start_ms ))
    local limit=$(( SCENARIO_TIMEOUT * 1000 ))
    if [[ $elapsed -gt $limit ]]; then
        log_error "Scenario timeout exceeded: ${elapsed}ms > ${limit}ms"
        return 1
    fi
    return 0
}

# macOS/Linux 호환 timeout 래퍼
run_timeout() {
    local secs="$1"; shift
    if command -v gtimeout &>/dev/null; then
        gtimeout "$secs" "$@"
    elif command -v timeout &>/dev/null; then
        timeout "$secs" "$@"
    else
        "$@"
    fi
}
