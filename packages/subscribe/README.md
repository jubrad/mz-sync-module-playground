# @mz-web/subscribe

Core Materialize subscribe client for JavaScript/TypeScript. Tracks progress cursors, batches events between progress boundaries, handles reconnection, and delivers atomic batches to registered sinks.

## Install

```bash
npm install @mz-web/subscribe
```

## Usage

```typescript
import { MzSubscribeClient } from '@mz-web/subscribe'
import { MzHttpTransport } from '@mz-web/transport-http'

const client = new MzSubscribeClient({
  transport: new MzHttpTransport({ baseUrl: 'http://localhost:8080' }),
})

// Register a sink to receive batches
client.addSink({
  onBatch(batch) {
    console.log(`${batch.view}: ${batch.upserts.size} upserts, ${batch.deletes.size} deletes`)
  },
})

// Start subscribing to views
client.addView('live_orders')
client.addView('store_dashboard')
```

## Concepts

### Batches

Events from Materialize SUBSCRIBE arrive as individual upserts, deletes, and progress markers. This client accumulates events between progress boundaries and delivers them as atomic `MzBatch` objects:

```typescript
interface MzBatch {
  view: string
  upserts: Map<string, Record<string, unknown>>
  deletes: Set<string>
  progress: number  // MZ timestamp that sealed this batch
  kind: 'snapshot' | 'catchup' | 'progress'
}
```

Within a batch, events are collapsed:
- Multiple upserts to the same key → last value wins
- Upsert then delete → only in deletes
- Delete then upsert → only in upserts

### Pluggable Transports

The client doesn't know how to connect — it delegates to a `MzTransportProvider`:

```typescript
interface MzTransportProvider {
  connect(view: string, opts: ConnectOpts): MzViewStream
}

interface MzViewStream {
  events: AsyncIterable<MzEvent>
  close(): void
}
```

Built-in transports:
- `@mz-web/transport-http` — WebSocket/SSE to mz-http server
- Future: `@mz-web/transport-direct` — Materialize native WebSocket

### Sinks

Register one or more sinks to process batches:

```typescript
interface MzSink {
  onBatch(batch: MzBatch): void | Promise<void>
  onConnectionChange?(view: string, connected: boolean): void
}
```

Built-in sinks:
- `@mz-web/sink-sqlite` — writes to SQLite
- `@mz-web/sink-tanstack` — feeds TanStack DB collections

### Progress Tracking & Reconnection

The client tracks the last progress timestamp per view. On reconnect, it sends `?since=<timestamp>` for efficient changelog catchup. If the server has compacted past that point, it falls back to a full snapshot.

Reconnection uses exponential backoff (default 2s → max 60s).

## API

### `MzSubscribeClient`

```typescript
new MzSubscribeClient(opts: {
  transport: MzTransportProvider
  views?: string[]           // initial views
  reconnectMs?: number       // default 2000
  maxReconnectMs?: number    // default 60000
})
```

| Method | Description |
|--------|-------------|
| `addSink(sink)` | Register a sink. Returns unsubscribe function. |
| `addView(view)` | Start subscribing. Ref-counted. Returns `ViewHandle`. |
| `removeView(view)` | Decrement ref count. Tears down at 0. |
| `getView(view)` | Get view metadata (progress, ready, connected). |
| `getViews()` | Get all active view handles. |
| `resync()` | Force full resync of all views. |
| `destroy()` | Tear down everything. |

### `ViewHandle`

```typescript
interface ViewHandle {
  readonly view: string
  readonly progress: number
  readonly ready: boolean
  readonly connected: boolean
}
```

### `BatchAccumulator`

Low-level helper for building batches from events. Used internally by the client, but exported for custom implementations:

```typescript
const acc = new BatchAccumulator('my_view')
acc.add({ type: 'upsert', key: '1', value: { name: 'alice' } })
acc.add({ type: 'upsert', key: '2', value: { name: 'bob' } })
const batch = acc.seal(timestamp)
```
