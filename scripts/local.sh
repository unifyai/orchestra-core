#!/usr/bin/env bash
# orchestra-core local bring-up.
#
# Spins up a Postgres+pgvector container, applies the kernel alembic chain,
# and boots a backgrounded uvicorn server. Self-contained
# `start | stop | restart | status` control surface for local single-tenant
# use, including by external tools (notably Unity's `scripts/setup.sh`)
# which parse the `export UNIFY_BASE_URL=... / export UNIFY_KEY=...` lines
# emitted on a successful start.
#
#   bash scripts/local.sh start
#   bash scripts/local.sh stop
#   bash scripts/local.sh restart
#   bash scripts/local.sh status
#
# Environment overrides:
#   ORCHESTRA_DB_HOST, ORCHESTRA_DB_PORT, ORCHESTRA_DB_USER,
#   ORCHESTRA_DB_PASS, ORCHESTRA_DB_BASE
#   ORCHESTRA_API_KEY      bearer token required for /v0/* (default: local-dev-key)
#   ORCHESTRA_PORT         uvicorn port (default: 8000)
#   ORCHESTRA_HOST         uvicorn host (default: 127.0.0.1)
#   ORCHESTRA_CONTAINER    docker container name (default: orchestra-core-pg)
#   ORCHESTRA_INACTIVITY_TIMEOUT_SECONDS  idle timeout before auto-shutdown (default: unset)
#
# `ORCHESTRA_DB_PORT` is used both as the host port the Postgres container
# binds to and as the port the app connects on. They are intentionally the
# same — Postgres inside the container always listens on 5432, the host
# port is what we advertise to the app + alembic.

set -euo pipefail

ORCHESTRA_CONTAINER="${ORCHESTRA_CONTAINER:-orchestra-core-pg}"
ORCHESTRA_DB_USER="${ORCHESTRA_DB_USER:-orchestra}"
ORCHESTRA_DB_PASS="${ORCHESTRA_DB_PASS:-orchestra}"
ORCHESTRA_DB_BASE="${ORCHESTRA_DB_BASE:-orchestra_core}"
ORCHESTRA_DB_HOST="${ORCHESTRA_DB_HOST:-localhost}"
ORCHESTRA_DB_PORT="${ORCHESTRA_DB_PORT:-55432}"
ORCHESTRA_API_KEY="${ORCHESTRA_API_KEY:-local-dev-key}"
ORCHESTRA_HOST="${ORCHESTRA_HOST:-127.0.0.1}"
ORCHESTRA_PORT="${ORCHESTRA_PORT:-8000}"
ORCHESTRA_OTEL="${ORCHESTRA_OTEL:-false}"

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

# Resolve a Python interpreter that has the project's deps installed.
# Prefer (in order): an in-project venv, `poetry run python`, then bare
# `python` / `python3`. This makes the script work both under `poetry
# install --in-project` and under the system poetry's centralised venv.
resolve_python() {
    if [[ -x "$repo_root/.venv/bin/python" ]]; then
        echo "$repo_root/.venv/bin/python"
        return 0
    fi
    if command -v poetry >/dev/null 2>&1; then
        local venv_path
        venv_path="$(poetry env info --path 2>/dev/null || true)"
        if [[ -n "$venv_path" && -x "$venv_path/bin/python" ]]; then
            echo "$venv_path/bin/python"
            return 0
        fi
    fi
    if command -v python >/dev/null 2>&1; then
        echo "python"
        return 0
    fi
    if command -v python3 >/dev/null 2>&1; then
        echo "python3"
        return 0
    fi
    echo ""
    return 1
}

PY="$(resolve_python)"
if [[ -z "$PY" ]]; then
    echo "Could not find a Python interpreter. Run \`poetry install\` first." >&2
    exit 1
fi

PIDFILE="$repo_root/.local-server.pid"
LOGFILE="$repo_root/.local-server.log"
LOCAL_URL="http://${ORCHESTRA_HOST}:${ORCHESTRA_PORT}/v0"

# ------------------------------------------------------------------ helpers

log()      { printf '%s\n' "$*" >&2; }
log_ok()   { printf '\033[32m✓\033[0m %s\n' "$*" >&2; }
log_err()  { printf '\033[31m✗\033[0m %s\n' "$*" >&2; }

is_db_running() {
    docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "$ORCHESTRA_CONTAINER"
}

is_server_running() {
    [[ -f "$PIDFILE" ]] || return 1
    local pid
    pid="$(cat "$PIDFILE" 2>/dev/null || true)"
    [[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null
}

wait_for_health() {
    # Poll /v0/health until it responds or `timeout` seconds elapse.
    local timeout="${1:-30}"
    local start now
    start="$(date +%s)"
    while true; do
        if curl -fsS -m 2 "${LOCAL_URL}/health" >/dev/null 2>&1; then
            return 0
        fi
        now="$(date +%s)"
        if (( now - start >= timeout )); then
            return 1
        fi
        sleep 1
    done
}

# ------------------------------------------------------------------ db

start_db() {
    if docker ps -a --format '{{.Names}}' 2>/dev/null | grep -qx "$ORCHESTRA_CONTAINER"; then
        docker start "$ORCHESTRA_CONTAINER" >/dev/null
    else
        docker run -d --name "$ORCHESTRA_CONTAINER" \
            -e "POSTGRES_USER=$ORCHESTRA_DB_USER" \
            -e "POSTGRES_PASSWORD=$ORCHESTRA_DB_PASS" \
            -e "POSTGRES_DB=$ORCHESTRA_DB_BASE" \
            -p "${ORCHESTRA_DB_PORT}:5432" \
            pgvector/pgvector:pg15 >/dev/null
    fi

    for _ in $(seq 1 30); do
        if docker exec "$ORCHESTRA_CONTAINER" pg_isready -U "$ORCHESTRA_DB_USER" >/dev/null 2>&1; then
            log_ok "Postgres ready ($ORCHESTRA_CONTAINER on host port $ORCHESTRA_DB_PORT)"
            return 0
        fi
        sleep 1
    done
    log_err "Postgres did not become ready in 30s"
    exit 1
}

stop_db() {
    docker stop "$ORCHESTRA_CONTAINER" >/dev/null 2>&1 || true
    docker rm "$ORCHESTRA_CONTAINER" >/dev/null 2>&1 || true
    log_ok "Postgres container removed"
}

# ------------------------------------------------------------------ alembic

run_migrations() {
    log "Running alembic upgrade head..."
    ORCHESTRA_DB_HOST="$ORCHESTRA_DB_HOST" \
    ORCHESTRA_DB_PORT="$ORCHESTRA_DB_PORT" \
    ORCHESTRA_DB_USER="$ORCHESTRA_DB_USER" \
    ORCHESTRA_DB_PASS="$ORCHESTRA_DB_PASS" \
    ORCHESTRA_DB_BASE="$ORCHESTRA_DB_BASE" \
    ORCHESTRA_OTEL="$ORCHESTRA_OTEL" \
    "$PY" -m alembic -c alembic.ini upgrade head >&2
    log_ok "Migrations applied"
}

# ------------------------------------------------------------------ uvicorn

start_server() {
    if is_server_running; then
        log_ok "Server already running (pid $(cat "$PIDFILE"))"
        return 0
    fi

    log "Starting uvicorn (logs -> $LOGFILE)..."
    # Background uvicorn so the script returns once the server is healthy.
    # `setsid` (where available) detaches the child into its own session so
    # it survives the parent shell exiting.
    local launcher="bash -c"
    if command -v setsid >/dev/null 2>&1; then
        launcher="setsid bash -c"
    fi

    ORCHESTRA_DB_HOST="$ORCHESTRA_DB_HOST" \
    ORCHESTRA_DB_PORT="$ORCHESTRA_DB_PORT" \
    ORCHESTRA_DB_USER="$ORCHESTRA_DB_USER" \
    ORCHESTRA_DB_PASS="$ORCHESTRA_DB_PASS" \
    ORCHESTRA_DB_BASE="$ORCHESTRA_DB_BASE" \
    ORCHESTRA_API_KEY="$ORCHESTRA_API_KEY" \
    ORCHESTRA_HOST="$ORCHESTRA_HOST" \
    ORCHESTRA_PORT="$ORCHESTRA_PORT" \
    ORCHESTRA_OTEL="$ORCHESTRA_OTEL" \
    ${ORCHESTRA_INACTIVITY_TIMEOUT_SECONDS:+ORCHESTRA_INACTIVITY_TIMEOUT_SECONDS="$ORCHESTRA_INACTIVITY_TIMEOUT_SECONDS"} \
    $launcher "exec '$PY' -m orchestra_core" >"$LOGFILE" 2>&1 &
    local pid=$!
    disown "$pid" 2>/dev/null || true
    echo "$pid" >"$PIDFILE"

    if wait_for_health 30; then
        log_ok "uvicorn ready on $LOCAL_URL"
        return 0
    fi

    log_err "uvicorn did not become healthy within 30s. Last log lines:"
    tail -20 "$LOGFILE" >&2 || true
    rm -f "$PIDFILE"
    exit 1
}

stop_server() {
    if [[ -f "$PIDFILE" ]]; then
        local pid
        pid="$(cat "$PIDFILE" 2>/dev/null || true)"
        if [[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null; then
            kill "$pid" 2>/dev/null || true
            sleep 1
            kill -9 "$pid" 2>/dev/null || true
        fi
        rm -f "$PIDFILE"
        log_ok "uvicorn stopped"
    fi
}

# ------------------------------------------------------------------ env block

emit_env_block() {
    # Lines below are intentionally unindented and prefixed with `export`
    # so external tools (e.g. Unity's `setup.sh`) can grep them via
    # `^export (UNIFY_BASE_URL|UNIFY_KEY)=` and source them with `eval`.
    cat <<EOF

orchestra-core ready: $LOCAL_URL
  health: ${LOCAL_URL}/health
  api key (UNIFY_KEY): $ORCHESTRA_API_KEY
  logs: $LOGFILE

To use in your shell:
  export UNIFY_BASE_URL='$LOCAL_URL'
  export UNIFY_KEY='$ORCHESTRA_API_KEY'

Or source this script:
  eval "\$(bash scripts/local.sh start | tail -2)"

EOF
    # Final two lines are what tools parse — keep them literal and last.
    printf "export UNIFY_BASE_URL='%s'\n" "$LOCAL_URL"
    printf "export UNIFY_KEY='%s'\n" "$ORCHESTRA_API_KEY"
}

# ------------------------------------------------------------------ commands

cmd_start() {
    start_db
    run_migrations
    start_server
    emit_env_block
}

cmd_stop() {
    stop_server
    stop_db
}

cmd_restart() {
    stop_server
    cmd_start
}

cmd_status() {
    if is_db_running; then
        log_ok "Postgres: running ($ORCHESTRA_CONTAINER)"
    else
        log_err "Postgres: not running"
    fi
    if is_server_running; then
        log_ok "uvicorn: running (pid $(cat "$PIDFILE"))"
        if curl -fsS -m 2 "${LOCAL_URL}/health" >/dev/null 2>&1; then
            log_ok "health: 200 OK at ${LOCAL_URL}/health"
        else
            log_err "health: not responding at ${LOCAL_URL}/health"
        fi
    else
        log_err "uvicorn: not running"
    fi
}

cmd="${1:-start}"
case "$cmd" in
    start)   cmd_start ;;
    stop)    cmd_stop ;;
    restart) cmd_restart ;;
    status)  cmd_status ;;
    *)
        echo "usage: $0 {start|stop|restart|status}" >&2
        exit 2
        ;;
esac
