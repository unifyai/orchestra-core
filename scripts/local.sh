#!/usr/bin/env bash
# orchestra-core local bring-up.
#
# Spins up a Postgres+pgvector container, applies the kernel alembic chain,
# and boots uvicorn. Self-contained `start | stop | restart` control surface
# for local single-tenant use.
#
#   bash scripts/local.sh start
#   bash scripts/local.sh stop
#   bash scripts/local.sh restart
#
# Environment overrides:
#   ORCHESTRA_DB_HOST, ORCHESTRA_DB_PORT, ORCHESTRA_DB_USER,
#   ORCHESTRA_DB_PASS, ORCHESTRA_DB_BASE
#   ORCHESTRA_API_KEY      bearer token required for /v0/* (default: local-dev-key)
#   ORCHESTRA_PORT         uvicorn port (default: 8000)
#   ORCHESTRA_CONTAINER    docker container name (default: orchestra-core-pg)
#   ORCHESTRA_DB_PORT_HOST host port to bind Postgres on (default: 55432)

set -euo pipefail

ORCHESTRA_CONTAINER="${ORCHESTRA_CONTAINER:-orchestra-core-pg}"
ORCHESTRA_DB_USER="${ORCHESTRA_DB_USER:-orchestra}"
ORCHESTRA_DB_PASS="${ORCHESTRA_DB_PASS:-orchestra}"
ORCHESTRA_DB_BASE="${ORCHESTRA_DB_BASE:-orchestra_core}"
ORCHESTRA_DB_HOST="${ORCHESTRA_DB_HOST:-localhost}"
ORCHESTRA_DB_PORT="${ORCHESTRA_DB_PORT:-55432}"
ORCHESTRA_DB_PORT_HOST="${ORCHESTRA_DB_PORT_HOST:-${ORCHESTRA_DB_PORT}}"
ORCHESTRA_API_KEY="${ORCHESTRA_API_KEY:-local-dev-key}"
ORCHESTRA_PORT="${ORCHESTRA_PORT:-8000}"
ORCHESTRA_OTEL="${ORCHESTRA_OTEL:-false}"

cmd="${1:-start}"

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

start_db() {
    if docker ps -a --format '{{.Names}}' | grep -qx "$ORCHESTRA_CONTAINER"; then
        docker start "$ORCHESTRA_CONTAINER" >/dev/null
    else
        docker run -d --name "$ORCHESTRA_CONTAINER" \
            -e "POSTGRES_USER=$ORCHESTRA_DB_USER" \
            -e "POSTGRES_PASSWORD=$ORCHESTRA_DB_PASS" \
            -e "POSTGRES_DB=$ORCHESTRA_DB_BASE" \
            -p "${ORCHESTRA_DB_PORT_HOST}:5432" \
            pgvector/pgvector:pg15 >/dev/null
    fi

    for _ in $(seq 1 30); do
        if docker exec "$ORCHESTRA_CONTAINER" pg_isready -U "$ORCHESTRA_DB_USER" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    echo "Postgres did not become ready in 30s" >&2
    exit 1
}

stop_db() {
    docker stop "$ORCHESTRA_CONTAINER" >/dev/null 2>&1 || true
    docker rm "$ORCHESTRA_CONTAINER" >/dev/null 2>&1 || true
}

run_migrations() {
    ORCHESTRA_DB_HOST="$ORCHESTRA_DB_HOST" \
    ORCHESTRA_DB_PORT="$ORCHESTRA_DB_PORT" \
    ORCHESTRA_DB_USER="$ORCHESTRA_DB_USER" \
    ORCHESTRA_DB_PASS="$ORCHESTRA_DB_PASS" \
    ORCHESTRA_DB_BASE="$ORCHESTRA_DB_BASE" \
    ORCHESTRA_OTEL="$ORCHESTRA_OTEL" \
    python -m alembic -c alembic.ini upgrade head
}

run_uvicorn() {
    ORCHESTRA_DB_HOST="$ORCHESTRA_DB_HOST" \
    ORCHESTRA_DB_PORT="$ORCHESTRA_DB_PORT" \
    ORCHESTRA_DB_USER="$ORCHESTRA_DB_USER" \
    ORCHESTRA_DB_PASS="$ORCHESTRA_DB_PASS" \
    ORCHESTRA_DB_BASE="$ORCHESTRA_DB_BASE" \
    ORCHESTRA_API_KEY="$ORCHESTRA_API_KEY" \
    ORCHESTRA_PORT="$ORCHESTRA_PORT" \
    ORCHESTRA_OTEL="$ORCHESTRA_OTEL" \
    python -m orchestra_core
}

case "$cmd" in
    start)
        start_db
        run_migrations
        echo
        echo "orchestra-core is starting on http://${ORCHESTRA_DB_HOST}:${ORCHESTRA_PORT}"
        echo "  ORCHESTRA_URL=http://${ORCHESTRA_DB_HOST}:${ORCHESTRA_PORT}/v0"
        echo "  UNIFY_KEY=${ORCHESTRA_API_KEY}"
        echo
        run_uvicorn
        ;;
    stop)
        stop_db
        ;;
    restart)
        stop_db
        start_db
        run_migrations
        run_uvicorn
        ;;
    *)
        echo "usage: $0 {start|stop|restart}" >&2
        exit 2
        ;;
esac
