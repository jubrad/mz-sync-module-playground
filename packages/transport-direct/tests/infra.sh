#!/usr/bin/env bash
set -euo pipefail

# Manages a standalone Materialize container for integration tests.
# No Postgres, no demo schema — just bare MZ.
#
# Usage:
#   ./tests/infra.sh up      # start MZ
#   ./tests/infra.sh down    # stop and remove
#   ./tests/infra.sh test    # start MZ, run tests, stop MZ
#   ./tests/infra.sh status  # check if MZ is running

CONTAINER_NAME="mz-transport-test"
MZ_SQL_PORT="${MZ_SQL_PORT:-6878}"
MZ_HTTP_PORT="${MZ_HTTP_PORT:-6876}"
MZ_IMAGE="${MZ_IMAGE:-materialize/materialized:latest}"

up() {
    if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        echo "MZ already running (${CONTAINER_NAME})"
        return
    fi

    echo "Starting Materialize (SQL: ${MZ_SQL_PORT}, HTTP/WS: ${MZ_HTTP_PORT})..."
    docker run -d \
        --name "${CONTAINER_NAME}" \
        -p "${MZ_SQL_PORT}:6875" \
        -p "${MZ_HTTP_PORT}:6876" \
        -e MZ_NO_TELEMETRY=1 \
        "${MZ_IMAGE}" >/dev/null

    echo "Waiting for MZ to be ready..."
    for i in $(seq 1 30); do
        if psql "postgres://materialize@localhost:${MZ_SQL_PORT}/materialize" -c "SELECT 1" &>/dev/null 2>&1; then
            echo "MZ ready (SQL: ${MZ_SQL_PORT}, HTTP/WS: ${MZ_HTTP_PORT})"
            return
        fi
        sleep 1
    done
    echo "ERROR: MZ failed to start within 30s"
    docker logs "${CONTAINER_NAME}" | tail -20
    exit 1
}

down() {
    if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        echo "Stopping ${CONTAINER_NAME}..."
        docker rm -f "${CONTAINER_NAME}" >/dev/null
        echo "Done"
    else
        echo "No container to stop"
    fi
}

status() {
    if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        echo "Running (SQL: ${MZ_SQL_PORT}, HTTP/WS: ${MZ_HTTP_PORT})"
        psql "postgres://materialize@localhost:${MZ_SQL_PORT}/materialize" -c "SELECT version()" 2>/dev/null | grep -o 'v[0-9].*' || true
    else
        echo "Not running"
    fi
}

run_tests() {
    up
    echo ""
    echo "Running integration tests..."
    MZ_HOST="localhost:${MZ_HTTP_PORT}" MZ_SQL_HOST="localhost:${MZ_SQL_PORT}" npx vitest run tests/integration.test.ts
    local exit_code=$?
    echo ""
    down
    exit $exit_code
}

case "${1:-help}" in
    up)     up ;;
    down)   down ;;
    status) status ;;
    test)   run_tests ;;
    *)
        echo "Usage: $0 {up|down|status|test}"
        echo ""
        echo "  up      Start a Materialize container for testing"
        echo "  down    Stop and remove the container"
        echo "  status  Check if MZ is running"
        echo "  test    Start MZ, run integration tests, stop MZ"
        echo ""
        echo "Environment:"
        echo "  MZ_PORT   Port to expose (default: 6878)"
        echo "  MZ_IMAGE  Docker image (default: materialize/materialized:latest)"
        ;;
esac
