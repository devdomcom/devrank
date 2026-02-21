#!/usr/bin/env bash
# Dev infrastructure helper — starts/stops PostgreSQL + Redis via Docker Compose.
# Automatically detects Docker-in-Docker (dev containers, Codespaces, Gitpod)
# and configures networking so services are reachable from the dev container.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Detect if running inside a container (DinD / dev container / Codespaces)
is_in_container() {
    [ -f /.dockerenv ] || [ -f /run/.containerenv ] || \
    grep -qE '(docker|containerd|kubepods)' /proc/1/cgroup 2>/dev/null || \
    grep -qE '(docker|containerd)' /proc/self/mountinfo 2>/dev/null || \
    [ -n "${REMOTE_CONTAINERS:-}" ] || [ -n "${CODESPACES:-}" ] || \
    [ -n "${GITPOD_WORKSPACE_ID:-}" ]
}

# Resolve the current container ID (best-effort; falls back to hostname)
get_container_id() {
    local cid=""
    # Try cgroup v1 first
    if [ -f /proc/self/cgroup ]; then
        cid=$(grep -oP '[a-f0-9]{64}' /proc/self/cgroup 2>/dev/null | head -1)
    fi
    # Try mountinfo (works on cgroup v2)
    if [ -z "$cid" ] && [ -f /proc/self/mountinfo ]; then
        cid=$(grep -oP '[a-f0-9]{64}' /proc/self/mountinfo 2>/dev/null | head -1)
    fi
    # Fallback: hostname is usually the short container ID
    echo "${cid:-$(hostname)}"
}

start() {
    cd "$PROJECT_ROOT"
    echo "Starting dev infrastructure..."

    # Guard: Docker must be available to start services
    if ! command -v docker &>/dev/null; then
        echo "Error: docker is not installed or not in PATH." >&2
        echo "If running inside a container with external DB/Redis, use --skip-infra" >&2
        echo "and set DEVRANK_DATABASE_URL / DEVRANK_REDIS_URL env vars." >&2
        exit 1
    fi

    docker compose up -d --wait

    if is_in_container; then
        echo "[DinD] Detected container environment"

        # Connect this container to the compose network so service names resolve
        CONTAINER_ID="$(get_container_id)"
        if [ -n "$CONTAINER_ID" ]; then
            local net_err
            if net_err=$(docker network connect devrank "$CONTAINER_ID" 2>&1); then
                echo "[DinD] Connected container to devrank network"
            elif echo "$net_err" | grep -q "already exists"; then
                echo "[DinD] Container already on devrank network"
            else
                echo "[DinD] Warning: could not connect to devrank network: $net_err"
            fi
        fi

        DEVRANK_DB_HOST=postgres
        DEVRANK_REDIS_HOST=redis
        echo "[DinD] Services reachable at postgres:5432 and redis:6379"
    else
        DEVRANK_DB_HOST=localhost
        DEVRANK_REDIS_HOST=localhost
        echo "[Native] Services reachable at localhost:5432 and localhost:6379"
    fi

    # Generate .env from template (.env.example) if it doesn't exist.
    # Substitutes DB/Redis hosts (localhost vs. postgres/redis for DinD).
    # Celery vars use DEVRANK_CELERY_* prefix (matches config.Settings; see refactor).
    # DB validators in config.py catch malformed URLs early.
    if [ ! -f "$PROJECT_ROOT/.env" ]; then
        if [ -f "$PROJECT_ROOT/.env.example" ]; then
            sed "s/DB_HOST/${DEVRANK_DB_HOST}/g; s/REDIS_HOST/${DEVRANK_REDIS_HOST}/g" \
                "$PROJECT_ROOT/.env.example" > "$PROJECT_ROOT/.env"
            echo "Created .env from template (host: ${DEVRANK_DB_HOST})"
        else
            echo "Warning: .env.example not found — skipping .env generation"
        fi
    else
        echo ".env already exists — skipping generation (delete it to regenerate)"
    fi

    echo ""
    echo "Ready! PostgreSQL: ${DEVRANK_DB_HOST}:5432  Redis: ${DEVRANK_REDIS_HOST}:6379"
}

stop() {
    cd "$PROJECT_ROOT"
    echo "Stopping dev infrastructure..."
    docker compose down
    echo "Stopped."
}

reset() {
    cd "$PROJECT_ROOT"
    echo "Stopping dev infrastructure and removing volumes..."
    docker compose down -v
    echo "Volumes removed. Run '$0 start' to recreate from scratch."
}

status() {
    cd "$PROJECT_ROOT"
    docker compose ps
}

case "${1:-start}" in
    start)  start ;;
    stop)   stop ;;
    reset)  reset ;;
    status) status ;;
    *)      echo "Usage: $0 {start|stop|reset|status}" ;;
esac
