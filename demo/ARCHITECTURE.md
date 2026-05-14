# Demo Architecture: Fresh Delivery

## Overview

Fresh Delivery is a real-time pizza delivery operations dashboard. It simulates a national delivery platform (like Uber Eats) that partners with independent pizza shops, tracks orders through their lifecycle, manages drivers, and surfaces operational issues — all powered by Materialize's incremental view maintenance.

```
┌──────────────┐     ┌───────────────┐     ┌──────────────┐     ┌────────────┐
│   Postgres   │────►│  Materialize  │────►│   mz-http    │────►│  Browser   │
│  (source of  │ CDC │  (7 views,    │ SUB │  (Rust/Axum) │ WS  │  (React +  │
│   truth)     │     │   IVM)        │     │  REST/WS/SSE │     │  SQLite)   │
└──────────────┘     └───────────────┘     └──────────────┘     └────────────┘
       ▲                                                              │
       │                                                              │
┌──────────────┐                                                      │
│  Simulator   │◄─────────────────────────────────────────────────────┘
│  (FastAPI)   │  POST /sim/stores, /sim/sale, /sim/drivers
│  tick() 4x/s │
└──────────────┘
```

## Components

### 1. PostgreSQL (source of truth)

Three tables with logical replication enabled:

```sql
stores  (id, name, city, state, zipcode, opened_at)
drivers (id, store_id, name, status, current_order_id, updated_at)
orders  (id, store_id, driver_id, customer_name, delivery_zipcode,
         status, items, total_cents, created_at, updated_at)
```

Order statuses: `placed → being_made → in_the_oven → final_prep → waiting_on_driver → on_its_way → delivered`

Publication: `demo_pub FOR ALL TABLES` with `REPLICA IDENTITY FULL`.

### 2. Materialize (incremental view maintenance)

Consumes Postgres CDC via a `POSTGRES` source. Maintains 7 materialized views incrementally — when a row changes in Postgres, only affected view outputs are recomputed.

#### Views

| View | Purpose | Key | Size |
|------|---------|-----|------|
| `live_orders` | Active orders + store/driver joins | `id` | ~100s-1000s rows |
| `store_dashboard` | Per-store aggregates (orders by status) | `id` | 1 row per store |
| `driver_board` | Driver status + current assignment | `id` | 1 row per driver |
| `recent_deliveries` | Recently completed (purged > 30s) | `id` | Bounded |
| `top_sellers` | Item combos ranked by count | `items` | ~12 rows |
| `slow_orders_by_store` | Stores with orders > 45s old | `store_id` | 0 to N stores |
| `slow_orders` | Individual slow orders | `id` | Bounded |

#### Temporal Views

`slow_orders` and `slow_orders_by_store` use Materialize's `mz_now()` temporal filter:

```sql
WHERE mz_now() >= o.created_at + INTERVAL '45 seconds'
```

This creates a time-dependent filter that MZ maintains incrementally — orders automatically enter the view when they become 45 seconds old and leave when delivered/deleted. No polling required.

### 3. mz-http (Rust/Axum server)

Connects to Materialize with one `SUBSCRIBE` cursor per view. Maintains an in-memory cache (DashMap) and broadcasts changes via:

- **WebSocket** `/api/ws/:name` — primary transport for the demo
- **SSE** `/api/sse/:name` — alternative transport
- **REST** `/api/views/:name` — snapshot, pagination, single-key lookup
- **Schema** `/api/views/:name/schema` — column names + PG types (used by SQLite table creation)

Each SUBSCRIBE produces upsert/delete/progress events that flow to connected clients.

#### Reconnection Protocol

Clients send `?since=<timestamp>` on reconnect. The server replays changes from its changelog ring buffer (100k entries). If the timestamp is too old (compacted), it falls back to a full snapshot + `snapshot_complete` event.

### 4. Simulator (Python/FastAPI)

Drives the demo by generating orders and advancing them through the pipeline.

**Tick loop** (every 250ms):
1. For each store, probabilistically create orders based on its rate (10-30/min)
2. Batch insert all new orders in one `executemany`
3. Call `tick()` stored procedure which advances order statuses

**Order lifecycle timing:**
- placed → being_made: 2-6s
- being_made → in_the_oven: 4-10s
- in_the_oven → final_prep: 6-14s
- final_prep → waiting_on_driver: 2-4s
- **on_its_way → delivered: 20-40s** (bottleneck — drivers are the constraint)

**Control API:**

| Endpoint | Purpose |
|----------|---------|
| `POST /sim/stores?count=N` | Add N partner stores (bulk insert) |
| `POST /sim/sale?multiplier=N` | Toggle sale (2x or 10x order rate) |
| `POST /sim/drivers/{id}?count=N` | Hire (+N) or fire (-N) drivers |
| `POST /sim/burst/{id}?count=N` | Send burst of orders to a store |
| `POST /sim/reset` | Wipe data, reset to initial 30 stores |
| `GET /sim/status` | Simulation stats |

**Store name generation:** Combinatorial from 38 prefixes × 27 nouns × 14 suffixes = ~14k unique pizza shop names (e.g., "Cosmic Crust", "Iron Flame Pizza", "Velvet Dough Pizza Co.").
### 5. Browser (React + SQLite)

The frontend maintains a full local replica of all 7 Materialize views in an in-memory SQLite database running in a Web Worker.

```
Main Thread                              Worker Thread
───────────                              ─────────────
7x MzTransport (WebSockets)              wa-sqlite WASM (:memory:)
     │                                        │
     ▼                                        │
MzSQLite                                      │
  pendingOps[] ──rAF/progress──►  postMessage ─┤
                                    BEGIN      │
                                    INSERT...  │
                                    COMMIT     │
                                        │      │
  useLiveQuery("SELECT...")             │      │
     │                                  │      │
     └──────── postMessage ────────────►│      │
                                  exec(sql, cb)│
               ◄── postMessage ────────┘       │
     setState(rows) → React render             │
```

**Dashboard layout:**
- Left sidebar: fuzzy-searchable store list with keyboard navigation
- "All Stores" view: top sellers, busiest stores, stores with slow orders (all from pre-computed MZ views)
- Store detail view: order pipeline, live orders table, drivers panel (hire/fire), slow orders

**Key optimization:** Queries against large tables (`live_orders`, `recent_deliveries`, `driver_board`) only run when a specific store is selected. The overview page queries only small pre-computed views.

## Data Volume at Scale

| Stores | Orders/sec | Active Orders | Drivers | Views synced/sec |
|--------|-----------|---------------|---------|-----------------|
| 30 | ~10 | ~300 | 210 | ~40 upserts |
| 100 | ~33 | ~1,000 | 700 | ~130 upserts |
| 500 | ~167 | ~5,000 | 3,500 | ~670 upserts |

With 2x sale: double the orders/sec. With 10x: ten times. The driver pool (7 per store, 20-40s delivery time) becomes the bottleneck — visible in the "Slow Orders" panel turning red.

## Running the Demo

```bash
cd demo
./run.sh
```

This builds everything, starts Postgres + Materialize in Docker, loads the schema, creates MZ views, starts the simulator, and serves the React app at `http://localhost:8080`.

**Controls in the UI:**
- **Add Stores**: type a number, click Add
- **Sale 2x / 10x**: increase order volume
- **+1 / +5 / -1 drivers**: manage driver pool per store
- **resync**: clear local SQLite and reconnect all views

## Infrastructure

```yaml
# docker-compose.yaml
postgres:    port 5488  (wal_level=logical)
materialize: port 6878  (MZ_WORKERS=10)

# processes
simulator:   port 9000  (FastAPI, 4 ticks/sec)
mz-http:     port 8080  (Rust/Axum, serves React app)
```
