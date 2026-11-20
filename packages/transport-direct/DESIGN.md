# @mz-web/transport-direct — Design

## Purpose

Connects directly to Materialize's native WebSocket API (`/api/experimental/sql`) without needing an mz-http proxy server. Implements the same `MzTransportProvider` interface as `@mz-web/transport-http`.

## Architecture

```
MzDirectTransport.connect(view, opts)
    │
    ▼
WebSocket to wss://<host>/api/experimental/sql
    │
    ├── Send auth: { user, password }
    │   ◄── ReadyForQuery
    │
    ├── Send: { query: "SUBSCRIBE (...) ENVELOPE UPSERT ..." }
    │   ◄── CommandStarting
    │   ◄── Rows (column metadata)
    │   ◄── Row [mz_timestamp, mz_progressed, mz_state, key, ...values]
    │   ◄── Row ...
    │   ◄── Row (progress: mz_progressed='t')
    │   ...
    │
    ▼
AsyncQueue<MzEvent> → MzViewStream.events

```

## Key Design Decisions

### 1. View Registration Required

Unlike the HTTP transport (where the server knows the SQL), the direct transport needs the client to specify what SQL to SUBSCRIBE to and which column is the key. This is done via `registerView()` before `connect()`.

This mirrors how mz-http's `config.yaml` works — the SQL and key_column are configuration, not runtime decisions.

### 2. ENVELOPE UPSERT with PROGRESS

Every SUBSCRIBE uses `ENVELOPE UPSERT (KEY ("...")) WITH (SNAPSHOT, PROGRESS)`:
- **ENVELOPE UPSERT** gives us clean insert/update/delete semantics with a single key column
- **SNAPSHOT** sends the current state on connect
- **PROGRESS** sends periodic progress markers that the subscribe client uses for batching

### 3. Row Parsing

Materialize's WebSocket API returns rows as arrays: `[mz_timestamp, mz_progressed, mz_state, key, ...values]`. The transport parses these into `MzEvent` objects matching the same format as `@mz-web/transport-http`:
- `mz_progressed = 't'` → `{ type: 'progress', timestamp }`
- `mz_state = 'upsert'` → `{ type: 'upsert', key, value: { col: val, ... } }`
- `mz_state = 'delete'` → `{ type: 'delete', key }`

The value object is built from the column metadata received in the `Rows` message.

### 4. Full Snapshot on Reconnection

The transport ignores the subscribe client's `since` timestamp and always sends `WITH (SNAPSHOT, PROGRESS)`. The subscribed SQL is a query, and resuming a query with `AS OF` rehydrates its dataflow and needs retained history on every input, so there is nothing cheaper to fall back on. An earlier version sent `AS OF <timestamp>` with the default `SNAPSHOT`, which re-sent every row but never the deletes that happened while disconnected.

The snapshot rows all carry the stream's first timestamp, and a progress row at that timestamp can precede them. The transport drops progress rows until one passes that timestamp and emits it as `snapshot_complete`. The subscribe client then deletes any key it holds that the snapshot did not contain.

### 5. One Connection Per View

Each `connect()` call opens a new WebSocket. This is the same model as the HTTP transport. The subscribe client manages multiple connections and handles reconnection.

## Message Flow

```
Client                    Materialize WS API
──────                    ──────────────────
connect()            ──►  WebSocket open
auth { user, pass }  ──►  
                     ◄──  ParameterStatus (multiple)
                     ◄──  BackendKeyData
                     ◄──  ReadyForQuery
                     
{ query: "SET...; 
  SUBSCRIBE..." }    ──►
                     ◄──  CommandStarting { has_rows: true }
                     ◄──  Rows { columns: [...] }
                     ◄──  Row [ts, 'f', 'upsert', '1', 'alice']  → MzEvent upsert
                     ◄──  Row [ts, 'f', 'upsert', '2', 'bob']    → MzEvent upsert
                     ◄──  Row [ts, 't', null, null, null]         → MzEvent progress
                     ◄──  Row [ts2, 'f', 'delete', '1', null]    → MzEvent delete
                     ◄──  Row [ts2, 't', null, null, null]        → MzEvent progress
                     ...  (streaming forever until close)
```

## CORS Considerations

Materialize Cloud may block WebSocket connections from unknown browser origins. This transport is best suited for:
- Node.js / Bun / Deno server-side applications
- Edge functions (Cloudflare Workers, etc.)
- CLI tools
- Environments where CORS is not a concern

For browser apps, use `@mz-web/transport-http` with an mz-http proxy that handles CORS.

## Future Enhancements

- **Connection pooling** — reuse a single WebSocket for multiple SUBSCRIBE queries (MZ supports this in Extended mode)
- **Schema probing** — auto-detect key column by running a PREPARE before SUBSCRIBE
- **Frontegg auth** — integrate with Materialize Cloud's Frontegg-based auth flow
