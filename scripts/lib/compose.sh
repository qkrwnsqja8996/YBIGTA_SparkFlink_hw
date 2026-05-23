#!/usr/bin/env bash
# =============================================================================
# Docker Compose Wrapper
# YBIGTA Spark & Flink HA Homework
# =============================================================================
# Source this file to get the compose() function with correct -f flags.

[[ -n "${_SPFL_COMPOSE_SH_LOADED:-}" ]] && return 0
_SPFL_COMPOSE_SH_LOADED=1

# Locate the project root (directory containing docker-compose.yml)
_find_project_root() {
    local dir
    dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
    echo "$dir"
}

PROJECT_ROOT="${PROJECT_ROOT:-$(_find_project_root)}"

COMPOSE_BASE="${PROJECT_ROOT}/docker-compose.yml"
COMPOSE_STUDENT="${PROJECT_ROOT}/student/docker-compose.student.yml"

# Validate compose files exist
if [[ ! -f "$COMPOSE_BASE" ]]; then
    echo "[ERROR] docker-compose.yml not found at $COMPOSE_BASE" >&2
    exit 1
fi

if [[ ! -f "$COMPOSE_STUDENT" ]]; then
    echo "[WARN]  student/docker-compose.student.yml not found, using base only" >&2
    _USE_STUDENT_COMPOSE=0
else
    _USE_STUDENT_COMPOSE=1
fi

# compose() - wrapper for docker compose with correct -f flags
# Usage: compose [docker-compose subcommand and args...]
# Example: compose up -d
#          compose ps
#          compose exec flink-jm bash
compose() {
    if [[ "$_USE_STUDENT_COMPOSE" == "1" ]]; then
        docker compose \
            -f "$COMPOSE_BASE" \
            -f "$COMPOSE_STUDENT" \
            "$@"
    else
        docker compose \
            -f "$COMPOSE_BASE" \
            "$@"
    fi
}

# Short alias for logging
compose_ps() {
    compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"
}
