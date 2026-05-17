#!/usr/bin/env bash
# =============================================================================
# Data Injection Script
# YBIGTA Spark & Flink HA Homework
# =============================================================================
# Produces deterministic events to Kafka and returns verification info.
#
# Usage:
#   ./scripts/inject_data.sh --scenario <id> --count <N> [--topic <topic>]
#
# Output JSON:
#   {
#     "count": <N>,
#     "sha256_of_expected_output": "<hex>",
#     "scenario_id": "<id>",
#     "topic": "<topic>"
#   }
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

source "${SCRIPT_DIR}/lib/common.sh"

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
SCENARIO_ID="0"
COUNT=1000
TOPIC="events"

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --scenario|-s)  SCENARIO_ID="$2"; shift 2 ;;
        --count|-n)     COUNT="$2";       shift 2 ;;
        --topic|-t)     TOPIC="$2";       shift 2 ;;
        --help|-h)
            printf 'Usage: %s --scenario <id> --count <N> [--topic <topic>]\n' "$0"
            exit 0
            ;;
        *) log_error "Unknown argument: $1"; exit 1 ;;
    esac
done

# ---------------------------------------------------------------------------
# Compute expected output
# ---------------------------------------------------------------------------
# Events are numbered 0..N-1, with event_type = event_type_{seq % 5}
# So for N events: counts are distributed round-robin across 5 types.

EXPECTED_SHA256=$(python3 - "$COUNT" <<'PYEOF'
import sys
import hashlib

n = int(sys.argv[1])
num_types = 5
counts = {}
for i in range(num_types):
    counts[f"event_type_{i}"] = n // num_types

# Distribute remainder to lower-indexed types
remainder = n % num_types
for i in range(remainder):
    counts[f"event_type_{i}"] += 1

# Build sorted output string matching flink_counter.py output format
lines = []
for et in sorted(counts.keys()):
    lines.append(f"{et}\t{counts[et]}")
output = "\n".join(lines) + "\n"
sha = hashlib.sha256(output.encode()).hexdigest()
print(sha)
PYEOF
)

log_info "Injecting $COUNT events to topic '$TOPIC' (scenario $SCENARIO_ID)"

# ---------------------------------------------------------------------------
# Produce events to Kafka
# Generate events as: "event_type_N:seq" lines
# ---------------------------------------------------------------------------
python3 - "$COUNT" "$TOPIC" "$SCENARIO_ID" <<'PYEOF'
import sys
import subprocess
import json

n = int(sys.argv[1])
topic = sys.argv[2]
scenario_id = sys.argv[3]

# Generate all messages
messages = []
for i in range(n):
    event_type = f"event_type_{i % 5}"
    msg = f"{event_type}:{i}"
    messages.append(msg)

# Produce to Kafka via docker exec
proc = subprocess.Popen(
    [
        "docker", "exec", "-i", "kafka",
        "kafka-console-producer",
        "--bootstrap-server", "localhost:29092",
        "--topic", topic,
    ],
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
)

stdin_data = "\n".join(messages).encode() + b"\n"
stdout, stderr = proc.communicate(stdin_data)

if proc.returncode != 0:
    print(f"[ERROR] kafka-console-producer failed: {stderr.decode()}", file=sys.stderr)
    sys.exit(1)
PYEOF

log_info "Injected $COUNT events successfully"

# ---------------------------------------------------------------------------
# Output JSON
# ---------------------------------------------------------------------------
printf '{"count":%d,"sha256_of_expected_output":"%s","scenario_id":"%s","topic":"%s"}\n' \
    "$COUNT" "$EXPECTED_SHA256" "$SCENARIO_ID" "$TOPIC"
