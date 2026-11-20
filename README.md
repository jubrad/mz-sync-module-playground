# mz-http

An HTTP serving layer for [Materialize](https://materialize.com). Uses `SUBSCRIBE` to hydrate in-memory state from materialized views and serves it over REST, SSE, WebSocket, and webhooks.

```
                          +------------------+
  Browser / Client  ----> |     mz-http      | ---SUBSCRIBE--> Materialize
    REST / SSE / WS       | (in-memory cache)|            (materialized views)
                          +------------------+
```

## JavaScript/TypeScript Packages

The [`packages/`](./packages/) directory contains modular `@mz-web/*` npm packages for building real-time frontends on Materialize — pluggable transports, SQLite/TanStack DB sinks, and React hooks. See the [packages README](./packages/README.md) for details.

## Demo: Fresh Delivery

The [`demo/`](./demo/) directory contains a full-stack pizza delivery operations dashboard built on this stack. It simulates a national delivery platform (like Uber Eats) with hundreds of partner stores, real-time order pipelines, driver management, and live operational alerts — all powered by Materialize incremental view maintenance synced to an in-browser SQLite database.

```bash
cd demo
./run.sh    # starts Postgres, Materialize, simulator, mz-http, and React frontend
```

See the [demo architecture doc](./demo/ARCHITECTURE.md) for the full system design.

## Quick Start

### 1. Build

```bash
cargo build --release
```

### 2. Build the React frontend (optional)

```bash
cd frontend
npm install
npm run build
cd ..
```

### 3. Configure

```bash
cp config.example.yaml config.yaml
# Edit config.yaml with your Materialize connection details
```

### 4. Run

```bash
# With React frontend
./target/release/mz-http --config config.yaml --static-dir static

# Or without frontend (API only)
./target/release/mz-http --config config.yaml
```

### 5. Open

- `http://localhost:8080` — React dashboard (if frontend was built)
- `http://localhost:8080/explorer.html` — Vanilla data explorer
- `http://localhost:8080/demo.html` — Vanilla API demo

### Frontend development

For hot-reload during frontend development, run the Vite dev server separately. It proxies API calls to mz-http:

```bash
# Terminal 1: backend
./target/release/mz-http --config config.yaml

# Terminal 2: frontend dev server
cd frontend
npm run dev
# Open http://localhost:3000
```

## Running the Demo

The demo spins up Postgres + Materialize in Docker, seeds sensor data, and runs everything:

```bash
cd demo
./run.sh
```

This will:
1. Build the Rust backend + React frontend
2. Start Postgres and Materialize via Docker Compose
3. Load a sensor/alerts schema with seed data
4. Create MZ source + materialized views
5. Set `default_timestamp_interval` to 100ms
6. Start mz-http serving the React app
7. Start a data generator that updates sensors every second

Open `http://localhost:8080` when it's ready. Ctrl-C tears everything down.

## Connecting to Materialize Cloud

In the [Materialize Console](https://console.materialize.com), go to **Connect** and grab your host, user, and app password.

```yaml
server:
  bind: "0.0.0.0:8080"

defaults:
  host: abc123.us-east-1.aws.materialize.cloud
  port: 6875
  user: you@example.com
  password: ${MZ_PASSWORD}   # env var expansion supported
  database: materialize
  tls: true                  # required for cloud
  fetch_size: 1024

views:
  - name: my_view
    sql: "SELECT * FROM my_materialized_view"
    key_column: id           # optional — auto-detects id > key > first column
    title_columns: [name, status]  # shown in live stream summaries
    cluster: default
```

### Env var expansion

Any `${VAR}` or `${VAR:-default}` in the config is expanded from environment variables:

```bash
export MZ_PASSWORD=mzp_abc123
./target/release/mz-http --config config.yaml
```

### Arbitrary SQL

You don't need to write `key`/`value` columns. Point the SQL at any view or query — mz-http auto-wraps it:

```yaml
views:
  - name: pr_status
    sql: >
      SELECT * FROM pr_tracker.current_state
      WHERE mz_now() >= updated_at
        AND mz_now() < updated_at + INTERVAL '10 days'
    key_column: pr_id
    title_columns: [pr_title, author, state]
```

mz-http probes the columns, picks the key, and wraps the SQL into `SELECT key, jsonb_build_object(...)::text AS value FROM (your_sql)` automatically.

### Per-view connection overrides

Views inherit from `defaults` but can override any field:

```yaml
views:
  - name: fast_view
    sql: "SELECT * FROM hot_data"
    cluster: serving

  - name: other_region
    sql: "SELECT * FROM regional_data"
    host: xyz789.eu-west-1.aws.materialize.cloud
    user: other@example.com
    password: ${OTHER_MZ_PASSWORD}
```

## API Reference

### REST

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Health check |
| `GET` | `/api/views` | List views with metadata, freshness, key counts |
| `GET` | `/api/views/:name` | Full snapshot as `{key: value, ...}` |
| `GET` | `/api/views/:name/list?q=&limit=50&offset=0&sort=key` | Paginated, searchable list |
| `GET` | `/api/views/:name/:key` | Single key lookup |

### Streaming

| Protocol | Path | Description |
|----------|------|-------------|
| SSE | `/api/sse/:name` | Server-Sent Events |
| WebSocket | `/api/ws/:name` | WebSocket |

Both support:
- `?snapshot=true` — send full state as upsert events, then live
- `?since=<timestamp>` — replay changelog since that progress timestamp, then live (falls back to snapshot if compacted)

Event format:
```json
{"type": "upsert", "view": "my_view", "key": "123", "value": "...", "timestamp": 1234567}
{"type": "delete", "view": "my_view", "key": "123", "timestamp": 1234568}
{"type": "progress", "view": "my_view", "timestamp": 1234569}
{"type": "catchup_complete", "mode": "changelog", "since": 1234500, "events": 42}
```

### Webhooks

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/webhooks` | Register a webhook |
| `GET` | `/api/webhooks` | List registered webhooks |
| `DELETE` | `/api/webhooks/:id` | Unregister |

Webhooks receive **rolled-up batches** on each progress boundary — changes to the same key within a window are collapsed to the final state:

```json
{
  "view": "sensors",
  "timestamp": 1234567,
  "changes": [
    {"type": "upsert", "key": "3", "value": "{...}"},
    {"type": "delete", "key": "7"}
  ]
}
```

No changes in a window = no POST. HMAC-SHA256 signing via `X-Webhook-Signature` when a `secret` is configured.

## React SDK

The `frontend/src/lib/` directory contains a React hooks library for building UIs on top of mz-http.

### Hooks

```tsx
import { MzProvider, useMzViews, useMzView, useMzKey } from './lib'

function App() {
  return (
    <MzProvider baseUrl="http://localhost:8080">
      <Dashboard />
    </MzProvider>
  )
}

function Dashboard() {
  // Poll view metadata.
  const { views, serverTimeMs, latencyMs } = useMzViews(3000)

  // Live-sync a view. Returns rows ready for TanStack Table.
  const { rows, columns, progress, ready, connected } = useMzView('sensors')

  // Single key REST lookup.
  const { value, loading } = useMzKey('sensors', '42')

  return <MzTable rows={rows} columns={columns} onRowClick={...} />
}
```

### `useMzView` output

| Field | Type | Description |
|-------|------|-------------|
| `rows` | `Array<{_key, ...fields}>` | Rows for TanStack Table |
| `columns` | `string[]` | Auto-discovered column names |
| `data` | `Map<string, T>` | Key-value map for direct lookups |
| `progress` | `number` | Last MZ progress timestamp |
| `ready` | `boolean` | Initial catchup complete |
| `connected` | `boolean` | SSE/WS is connected |
| `sync` | `MzSync` | Underlying sync engine |

### Sync engine

The core sync engine (`MzSync`) is framework-agnostic and can be used standalone:

```js
import { MzSync } from './lib/mz-sync'

const sync = new MzSync('http://localhost:8080', 'sensors', {
  transport: 'sse',  // or 'ws'
})
sync.on('upsert', (key, value) => { ... })
sync.on('ready', () => console.log(sync.size, 'keys'))
sync.connect()

// On reconnect, sends ?since=<last_progress> for changelog-based catchup.
// Falls back to full snapshot if the server has compacted past that point.
```

Also available as a vanilla JS module at `/mz-sync.js`.

## Architecture

```
config.yaml
    |
    v
 main.rs            -- CLI, config, startup
    |
    +-- pipeline.rs (x N views)
    |       |
    |       +-- subscribe.rs    -- SUBSCRIBE cursor, auto SQL wrapping
    |       |       |
    |       |       v
    |       |   Materialize (one TCP connection per view)
    |       |
    |       +-- store.rs        -- DashMap + changelog + broadcast per view
    |
    +-- server.rs               -- axum: REST / SSE / WS / webhooks
    |
    +-- frontend/               -- React SDK + demo app
            |
            +-- src/lib/        -- MzSync, hooks, context, types
            +-- src/components/ -- MzTable (TanStack Table wrapper)
            +-- src/demo/       -- Demo app
```

Each view gets:
- **TCP connection** to Materialize with a `SUBSCRIBE` cursor
- **DashMap** for lock-free concurrent reads
- **Changelog** ring buffer (100k entries) for reconnection catchup
- **broadcast channel** feeding SSE, WebSocket, and webhook consumers

## Materialize SDK

The server consumes `SUBSCRIBE` through the [Materialize SDK](https://github.com/MaterializeInc/materialize/tree/main/misc/materialize-sdk)'s Rust package (`LiveSubscription`). The SDK owns the cursor, checks the protocol, and releases changes as batches at closed frontiers; mz-http wraps each view's SQL into `key`/`value`, applies the batches to its store, and reconnects.

The SDK is unreleased. `Cargo.toml` pins a commit from its draft pull request stack (MaterializeInc/materialize#40807 to #40820), so the first build clones the Materialize repository, and the pin needs bumping as those PRs change. The TypeScript packages under `packages/` do not use the SDK yet: its Node package runs over `pg` and has no browser or WebSocket transport.

## Limitations

**A break in the Materialize connection costs a full snapshot.** Every view is subscribed to as a query (`SUBSCRIBE (<sql>)`, usually wrapped to produce `key`/`value`), and a subscription to a query cannot be resumed from a timestamp. When the connection drops, mz-http reconnects with a fresh snapshot instead of `AS OF`:

- The store keeps serving its previous contents until the new snapshot is complete, then applies it.
- Keys missing from the new snapshot are deleted and broadcast as `delete` events, so connected clients and changelog replays (`?since=`) converge.
- Rows that did not change are re-sent as upserts. A view with a large result pays for the whole result on every reconnect.

Clients of mz-http are not affected in the same way: they resume from the changelog, and only fall back to a snapshot when it has been compacted.

## Logging

```bash
RUST_LOG=mz_http=debug ./target/release/mz-http --config config.yaml
```
