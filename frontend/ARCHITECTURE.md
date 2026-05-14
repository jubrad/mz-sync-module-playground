# Frontend Architecture

## Overview

The frontend is a reactive local-first data layer for Materialize. It syncs materialized view output from Materialize via WebSocket, stores it in an in-memory SQLite database (running in a Web Worker), and exposes it to React components via live SQL queries.

```
Materialize          mz-http (Rust)         Browser
───────────          ──────────────         ───────
SUBSCRIBE  ───CDC──► Axum server   ──WS──► MzTransport (main thread)
  (IVM)              REST / SSE / WS          │
                     /api/views/:name         │ ChangeEvents
                     /api/ws/:name            ▼
                                          MzSQLite (main thread)
                                              │ postMessage
                                              ▼
                                          Web Worker
                                          ┌─────────────────────┐
                                          │  wa-sqlite (WASM)   │
                                          │  :memory: database  │
                                          │                     │
                                          │  INSERT OR REPLACE  │
                                          │  DELETE             │
                                          │  SELECT (queries)   │
                                          └─────────────────────┘
                                              │ postMessage
                                              ▼
                                          React hooks
                                          useLiveQuery("SELECT ...")
                                          useLiveQueryScalar(...)
```

## Module Map

```
src/lib/
├── types.ts              Type definitions (ChangeEvent, QueryOpts, etc.)
├── mz-sync.ts            MzTransport — SSE/WS connection layer
├── mz-store.ts           MzStore — in-memory Map store (alternative to SQLite)
├── mz-sqlite.ts          MzSQLite — main-thread SQLite client
├── mz-sqlite-worker.ts   Web Worker — owns wa-sqlite, executes SQL
├── mz-sqlite-hooks.ts    React hooks for SQLite (useLiveQuery, etc.)
├── hooks.ts              React hooks for MzStore (useMzQuery, etc.)
├── context.tsx            MzProvider — React context for MzStore
├── wa-sqlite.d.ts        Type declarations for wa-sqlite
└── index.ts              Public API barrel export
```

## Two Store Backends

The library provides two storage backends. Both use the same `MzTransport` for WebSocket/SSE connections.

### 1. MzStore (in-memory Map)

Lightweight, no WASM dependency. Good for simple use cases.

```
MzTransport ──events──► MzStore (Map per view)
                             │
                             ▼
                        useMzQuery()     ← useSyncExternalStore
                        useMzRow()
                        useMzCount()
```

- Data stored in `Map<string, T>` per view
- Queries are JS filter/sort functions, not SQL
- Structural sharing via `RowCache` (unchanged rows keep object identity)
- Progress-boundary batching + rAF flush
- localStorage persistence (visible tab only writes)

### 2. MzSQLite (wa-sqlite in Web Worker)

Full SQL query capability, offloads work from the main thread.

```
MzTransport ──events──► MzSQLite (main thread)
                             │ postMessage({type:'commit', ops})
                             ▼
                        Web Worker (mz-sqlite-worker.ts)
                             │ wa-sqlite WASM
                             │ INSERT OR REPLACE / DELETE
                             ▼
                        :memory: SQLite DB
                             │
                        postMessage({type:'result', rows})
                             │
                             ▼
                        useLiveQuery("SELECT ...") ← useState + subscribe
```

- Tables auto-created from `/api/views/:name/schema` or inferred from first row
- Writes batched: events buffer → rAF or progress → single BEGIN/COMMIT
- Queries via `sqlite.exec()` with callback
- Main thread never blocks on SQLite

## Data Flow: Event to Render

```
1. WebSocket message arrives
   {"type":"upsert","key":"42","value":"{...}","timestamp":1234}

2. MzTransport.handleMessage() parses JSON, calls onEvent()

3. MzSQLite.handleEvent() buffers in pendingOps[]

4. On requestAnimationFrame OR progress event:
   MzSQLite.flushView() sends ops to worker via postMessage

5. Worker wraps in BEGIN ... INSERT OR REPLACE ... COMMIT
   Increments version, posts back {type:'committed', version}

6. MzSQLite.onWorkerMessage() updates version, calls notify()

7. notify() fires all subscribed callbacks

8. useLiveQuery's callback runs queryAsync(sql)
   → postMessage({type:'query', sql}) to worker
   → worker executes SQL, returns rows
   → setState({rows, version}) triggers React render
```

## Schema Discovery

Three modes for creating SQLite tables:

```typescript
// 1. Server (default) — fetches from /api/views/:name/schema
db.sync('live_orders')

// 2. Explicit — client-defined columns
db.sync('live_orders', {
  columns: { id: 'integer', name: 'text', total: 'integer' },
  primaryKey: 'id',
})

// 3. Infer — creates table from first upsert row
db.sync('live_orders', { schema: 'infer' })
```

PG types are mapped to SQLite: `int2/int4/int8/bool` → INTEGER, `float4/float8/numeric` → REAL, everything else → TEXT.

## Freshness Tracking

The `MzSQLite.getLastCommitTime()` returns the wall-clock `Date.now()` of the last successful commit. The UI samples this every 200ms over a 3-second rolling window to display average data freshness (e.g., "120ms unfresh").

## React Hook API

### SQLite hooks (used by the demo)

```typescript
// Live SQL query — re-runs on table commits
const { rows, version } = useLiveQuery<Order>(
  'SELECT * FROM live_orders WHERE store_id = ?',
  [42],
  ['live_orders']  // tables to subscribe to
)

// Scalar query
const count = useLiveQueryScalar<number>(
  'SELECT count(*) FROM live_orders',
  [],
  ['live_orders']
)
```

### MzStore hooks (alternative, no SQLite)

```typescript
// Filtered/sorted query subscription
const { rows, ready, connected } = useMzQuery<Order>('live_orders', {
  filter: (row) => row.status === 'placed',
  sort: (a, b) => b.total - a.total,
  limit: 50,
})

// Single row
const { value } = useMzRow<Order>('live_orders', '42')

// Count with filter
const placed = useMzCount<Order>('live_orders', (r) => r.status === 'placed')

// REST metadata polling
const { views, serverTimeMs, latencyMs } = useMzViews(3000)

// REST single-key lookup
const { value, loading } = useMzKey<Order>('live_orders', '42')
```

## Performance Characteristics

| Technique | What it does |
|-----------|-------------|
| **Web Worker** | SQLite runs off main thread — writes and queries don't block rendering |
| **Batch commits** | Multiple events per rAF frame → single BEGIN/COMMIT transaction |
| **Progress-boundary commits** | Materialize progress events trigger immediate commit (consistency) |
| **Conditional queries** | `useLiveQuery(null)` skips execution entirely — no SQL runs when not needed |
| **Prepared metadata** | Worker caches column names/pk per table, builds SQL from cached info |
| **PRAGMA synchronous=OFF** | No fsync for in-memory database |
| **useDeferredValue** | React defers non-critical panel re-renders under load |
