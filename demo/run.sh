#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

PIDS=()
cleanup() {
    echo "--- shutting down ---"
    for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done
    docker compose down -v 2>/dev/null || true
}
trap cleanup EXIT
trap 'exit 0' INT TERM

# ── 1. Build backend ────────────────────────────────────────────
echo "==> Building mz-http..."
(cd .. && cargo build --release 2>&1)

# ── 2. Build frontend ──────────────────────────────────────────
echo "==> Building React frontend..."
(cd ../frontend && npm install --silent && npm run build 2>&1)
cp -r ../frontend/dist/* ../static/ 2>/dev/null || true

# ── 3. Python venv + deps ──────────────────────────────────────
echo "==> Setting up Python venv..."
if [ ! -d .venv ]; then
    python3 -m venv .venv
fi
source .venv/bin/activate
pip install -q -r requirements.txt

# ── 4. Infrastructure ──────────────────────────────────────────
echo "==> Starting docker compose..."
docker compose down -v 2>/dev/null || true
docker compose up -d

# ── 5. Wait for Postgres ────────────────────────────────────────
echo "==> Waiting for Postgres (port 5488)..."
for i in $(seq 1 30); do
    if pg_isready -h localhost -p 5488 -U postgres -q 2>/dev/null; then break; fi
    sleep 1
done

# ── 6. Load schema ──────────────────────────────────────────────
echo "==> Loading schema..."
PGPASSWORD=postgres psql -h localhost -p 5488 -U postgres -d demo -f schema.sql

# ── 7. Wait for Materialize ────────────────────────────────────
echo "==> Waiting for Materialize (port 6878)..."
for i in $(seq 1 30); do
    if psql "postgres://materialize@localhost:6878/materialize" -c "SELECT 1" &>/dev/null; then break; fi
    sleep 1
done

# ── 8. Set tick interval to 100ms ──────────────────────────────
echo "==> Setting default_timestamp_interval to 1ms..."
psql "postgres://mz_system@localhost:6879/materialize" -c "ALTER SYSTEM SET default_timestamp_interval = '1ms';" 2>&1 | grep -E "^(ALTER|ERROR)" || true

# ── 9. Create MZ objects ───────────────────────────────────────
echo "==> Creating Materialize source + views..."
psql "postgres://materialize@localhost:6878/materialize" -f setup.sql

# ── 10. Start Python simulator ─────────────────────────────────
echo "==> Starting pizza order simulator (port 9000)..."
.venv/bin/python simulator.py &
PIDS+=($!)
sleep 2

# ── 11. Wait for snapshot hydration ────────────────────────────
echo "==> Waiting for MZ views to hydrate..."
for i in $(seq 1 60); do
    cnt=$(psql "postgres://materialize@localhost:6878/materialize" -tAc "SELECT count(*) FROM live_orders" 2>/dev/null || echo 0)
    if [ "$cnt" -gt 0 ]; then echo "  live_orders has $cnt rows"; break; fi
    sleep 2
done

# ── 12. Start mz-http ─────────────────────────────────────────
echo "==> Starting mz-http server (port 8080)..."
RUST_LOG=mz_http=info ../target/release/mz-http \
    --config config.yaml \
    --static-dir ../static &
PIDS+=($!)
sleep 1

# ── 13. Done ───────────────────────────────────────────────────
echo ""
echo "============================================"
echo "  Slice Command Operations Center"
echo ""
echo "  Dashboard:     http://localhost:8080"
echo "  Simulator API: http://localhost:9000/docs"
echo "  Open a store:  curl -X POST http://localhost:9000/sim/stores"
echo "  Sim status:    curl http://localhost:9000/sim/status"
echo "  API:           http://localhost:8080/api/views"
echo "============================================"
echo ""
echo "Press Ctrl-C to stop."
wait
