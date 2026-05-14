# @mz-web Package Plan

## Packages

### 1. `@mz-web/subscribe` — Core subscribe client
- Pluggable transport interface (`MzTransportProvider`)
- Tracks progress cursors per view
- Batches events between progress boundaries into atomic `MzBatch` objects
- Handles reconnection with backoff
- Determines snapshot vs changelog catchup
- Delivers batches to registered `MzSink` implementations
- Zero framework deps, zero storage deps

### 2. `@mz-web/transport-http` — mz-http WebSocket/SSE transport
- Implements `MzTransportProvider` for the mz-http server
- WebSocket (default) and SSE modes
- Converts push-based APIs (WS onmessage, SSE events) to `AsyncIterable<MzEvent>`
- URL construction, JSON parsing, query param handling (`?since=`, `?snapshot=true`)
- Future: `@mz-web/transport-direct` for Materialize's native WS subscribe API

### 3. `@mz-web/sink-sqlite` — Generic SQLite sink
- Implements `MzSink` to consume batches from subscribe client
- Writes to any SQLite via a minimal `SqliteExec` interface (`exec(sql)`, `query(sql, params)`)
- Schema discovery: server API, explicit, infer from first row
- PG → SQLite type mapping
- Batch SQL generation (BEGIN/INSERT OR REPLACE/DELETE/COMMIT)
- Per-table notification system for React hooks
- Works with: wa-sqlite (WASM), better-sqlite3 (Node), Bun SQLite, etc.

### 4. `@mz-web/sqlite-wasm` — Browser wa-sqlite runtime
- Loads wa-sqlite in a Web Worker
- Implements `SqliteExec` interface via postMessage proxy
- Dumb SQL executor — no table/schema awareness
- Browser-specific (only package that touches Worker APIs)
- PRAGMA optimizations (WAL, synchronous=OFF)

### 5. `@mz-web/sink-tanstack` — TanStack DB sink
- Creates TanStack DB collection options from Materialize views
- Maps `MzBatch` to `begin()/write()/commit()` + `markReady()`
- One collection per view
- Peer dep on `@tanstack/db`

### 6. `@mz-web/react` — React bindings
- `MzProvider` context (accepts subscribe client + optional SQLite sink)
- `useLiveQuery(sql, params, tables)` — live SQL against SQLite sink
- `useLiveQueryScalar(sql, params, tables)` — scalar query
- `useMzViews(intervalMs)` — REST metadata polling
- `useMzKey(view, key)` — REST single-key lookup
- `useFreshness()` — rolling average data freshness
- Peer dep on React >=18

## Transport Interface

```typescript
interface MzTransportProvider {
  connect(view: string, opts: ConnectOpts): MzViewStream
}

interface ConnectOpts {
  since?: MzTimestamp
  snapshot?: boolean
}

interface MzViewStream {
  events: AsyncIterable<MzEvent>
  close(): void
}

type MzEvent =
  | { type: 'upsert'; key: string; value: Record<string, unknown> | string }
  | { type: 'delete'; key: string }
  | { type: 'progress'; timestamp: MzTimestamp }
  | { type: 'snapshot_complete'; timestamp: MzTimestamp }
  | { type: 'catchup_complete'; timestamp: MzTimestamp }
  | { type: 'lagged' }
```

## Sink Interface

```typescript
interface MzSink {
  onBatch(batch: MzBatch): void | Promise<void>
  onConnectionChange?(view: string, connected: boolean): void
}

interface MzBatch {
  view: string
  upserts: Map<string, Record<string, unknown>>
  deletes: Set<string>
  progress: MzTimestamp
  kind: 'snapshot' | 'catchup' | 'progress'
}
```

## Dependency Graph

```
@mz-web/subscribe          (zero deps)
    ▲
    ├── @mz-web/transport-http     (depends on subscribe types)
    │
    ├── @mz-web/sink-sqlite        (depends on subscribe types)
    │       ▲
    │       └── @mz-web/sqlite-wasm  (depends on sink-sqlite SqliteExec)
    │
    ├── @mz-web/sink-tanstack      (depends on subscribe types, peer: @tanstack/db)
    │
    └── @mz-web/react              (depends on subscribe types, optional: sink-sqlite)
```

## Implementation Order

1. `@mz-web/subscribe` — foundation, no deps
2. `@mz-web/transport-http` — depends on subscribe
3. `@mz-web/sink-sqlite` — depends on subscribe
4. `@mz-web/sqlite-wasm` — depends on sink-sqlite
5. `@mz-web/sink-tanstack` — depends on subscribe
6. `@mz-web/react` — depends on subscribe + sink-sqlite

## Directory Structure

```
packages/
├── subscribe/
│   ├── src/
│   │   ├── index.ts
│   │   ├── types.ts
│   │   ├── transport.ts
│   │   ├── batch.ts
│   │   └── subscribe-client.ts
│   ├── tests/
│   ├── package.json, tsconfig.json, vitest.config.ts
│   ├── README.md
│   └── DESIGN.md
├── transport-http/
│   ├── src/
│   │   ├── index.ts
│   │   ├── http-transport.ts
│   │   ├── ws-stream.ts
│   │   ├── sse-stream.ts
│   │   └── async-queue.ts
│   ├── tests/
│   ├── package.json, tsconfig.json, vitest.config.ts
│   ├── README.md
│   └── DESIGN.md
├── sink-sqlite/
│   ├── src/
│   │   ├── index.ts
│   │   ├── types.ts
│   │   ├── sqlite-sink.ts
│   │   ├── schema.ts
│   │   └── table-writer.ts
│   ├── tests/
│   ├── package.json, tsconfig.json, vitest.config.ts
│   ├── README.md
│   └── DESIGN.md
├── sqlite-wasm/
│   ├── src/
│   │   ├── index.ts
│   │   ├── wasm-sqlite.ts
│   │   └── worker.ts
│   ├── tests/
│   ├── package.json, tsconfig.json, vitest.config.ts
│   ├── README.md
│   └── DESIGN.md
├── sink-tanstack/
│   ├── src/
│   │   ├── index.ts
│   │   └── tanstack-sink.ts
│   ├── tests/
│   ├── package.json, tsconfig.json, vitest.config.ts
│   ├── README.md
│   └── DESIGN.md
└── react/
    ├── src/
    │   ├── index.ts
    │   ├── context.tsx
    │   ├── use-live-query.ts
    │   ├── use-live-query-scalar.ts
    │   ├── use-mz-views.ts
    │   ├── use-mz-key.ts
    │   └── use-freshness.ts
    ├── tests/
    ├── package.json, tsconfig.json, vitest.config.ts
    ├── README.md
    └── DESIGN.md
```
