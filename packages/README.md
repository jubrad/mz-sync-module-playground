# @mz-web packages

A modular TypeScript toolkit for building real-time applications on [Materialize](https://materialize.com). Syncs materialized view output into local stores via WebSocket, with pluggable transports, storage backends, and framework bindings.

```
Materialize ──SUBSCRIBE──► Transport ──► Subscribe Client ──► Sink ──► UI
```

## Packages

### Core

| Package | Description |
|---------|-------------|
| **[@mz-web/subscribe](./subscribe)** | Core subscribe client. Tracks progress cursors, batches events between progress boundaries, handles reconnection with jitter. Delivers atomic `MzBatch` objects to registered sinks. Zero dependencies. |

### Transports

| Package | Description |
|---------|-------------|
| **[@mz-web/transport-http](./transport-http)** | WebSocket/SSE transport for [mz-http](../README.md) servers. Connects to `/api/ws/:view` or `/api/sse/:view`. Best for browser apps (CORS-friendly). |
| **[@mz-web/transport-direct](./transport-direct)** | Direct transport from Materialize's native WebSocket API (`/api/experimental/sql`). Authenticates, sends `SUBSCRIBE ... ENVELOPE UPSERT`, parses row results. Best for Node.js, edge functions, CLI tools. |

### Sinks

| Package | Description |
|---------|-------------|
| **[@mz-web/sink-sqlite](./sink-sqlite)** | Writes batches to any SQLite database via a minimal `SqliteExec` interface. Schema discovery (server, explicit, infer). SQL injection safe. Works with wa-sqlite, better-sqlite3, Bun, D1. |
| **[@mz-web/sink-tanstack](./sink-tanstack)** | Feeds batches into [TanStack DB](https://tanstack.com/db) collections via `begin()/write()/commit()`. Maps progress events to `markReady()`. |

### Runtimes

| Package | Description |
|---------|-------------|
| **[@mz-web/sqlite-wasm](./sqlite-wasm)** | Browser-specific wa-sqlite runtime. Runs SQLite in a Web Worker via WebAssembly. Implements the `SqliteExec` interface for sink-sqlite. |

### Framework Bindings

| Package | Description |
|---------|-------------|
| **[@mz-web/react](./react)** | React 18+ hooks: `useLiveQuery` (live SQL against local SQLite), `useLiveQueryScalar`, `useMzViews` (REST polling), `useMzKey` (single-key lookup), `useFreshness` (data staleness tracking). |

## Dependency Graph

```
@mz-web/subscribe              (zero deps — core types + client)
    ▲
    ├── @mz-web/transport-http   (WebSocket/SSE to mz-http proxy)
    ├── @mz-web/transport-direct (WebSocket from Materialize native API)
    │
    ├── @mz-web/sink-sqlite      (batch writer for any SQLite)
    │       ▲
    │       └── @mz-web/sqlite-wasm  (wa-sqlite in Web Worker)
    │
    ├── @mz-web/sink-tanstack    (TanStack DB collection sync)
    │
    └── @mz-web/react            (React hooks for SQLite + REST)
```

## Quick Start

### Browser (SQLite + React)

```typescript
import { MzSubscribeClient } from '@mz-web/subscribe'
import { MzHttpTransport } from '@mz-web/transport-http'
import { MzSqliteSink } from '@mz-web/sink-sqlite'
import { WasmSqlite } from '@mz-web/sqlite-wasm'
import { MzProvider, useLiveQuery } from '@mz-web/react'

// Setup
const transport = new MzHttpTransport({ baseUrl: 'http://localhost:8080' })
const client = new MzSubscribeClient({ transport })
const db = new WasmSqlite({ workerUrl: '/sqlite-worker.js' })
await db.init()
const sink = new MzSqliteSink({ db, schema: 'server', serverBaseUrl: 'http://localhost:8080' })
client.addSink(sink)
client.addView('live_orders')

// React
function App() {
  return (
    <MzProvider baseUrl="http://localhost:8080" client={client} sqliteSink={sink}>
      <OrderList />
    </MzProvider>
  )
}

function OrderList() {
  const { rows } = useLiveQuery(
    'SELECT * FROM live_orders WHERE status = ?',
    ['placed'],
    ['live_orders'],
  )
  return <ul>{rows.map(r => <li key={r.id}>{r.customer_name}</li>)}</ul>
}
```

### Node.js (Direct from Materialize)

```typescript
import { MzSubscribeClient } from '@mz-web/subscribe'
import { MzDirectTransport } from '@mz-web/transport-direct'

const transport = new MzDirectTransport({
  host: 'your-instance.materialize.cloud',
  auth: { user: 'you@example.com', password: 'mzp_...' },
  cluster: 'default',
})
transport.registerView('orders', { sql: 'SELECT * FROM orders_view', keyColumn: 'id' })

const client = new MzSubscribeClient({ transport })
client.addSink({
  onBatch(batch) {
    console.log(`${batch.view}: ${batch.upserts.size} upserts, ${batch.deletes.size} deletes`)
  },
})
client.addView('orders')
```

### TanStack DB

```typescript
import { MzSubscribeClient } from '@mz-web/subscribe'
import { MzHttpTransport } from '@mz-web/transport-http'
import { mzCollectionSync } from '@mz-web/sink-tanstack'
import { createCollection } from '@tanstack/db'

const transport = new MzHttpTransport({ baseUrl: 'http://localhost:8080' })
const client = new MzSubscribeClient({ transport })

const ordersCollection = createCollection({
  id: 'orders',
  getKey: (order) => order.id,
  ...mzCollectionSync(client, 'live_orders', { pk: 'id' }),
})
```

## Architecture

See [PLAN.md](./PLAN.md) for the full package design, dependency graph, and implementation details. Each package also has its own `DESIGN.md` with architecture decisions and trade-offs.
