# @mz-web/sink-sqlite

SQLite sink for `@mz-web/subscribe`. Writes Materialize view data to any SQLite database.

## Install

```bash
npm install @mz-web/sink-sqlite @mz-web/subscribe
```

## Usage

```typescript
import { MzSubscribeClient } from '@mz-web/subscribe'
import { MzHttpTransport } from '@mz-web/transport-http'
import { MzSqliteSink } from '@mz-web/sink-sqlite'

// Any SQLite that implements SqliteExec
import { WasmSqlite } from '@mz-web/sqlite-wasm'  // browser
// or: import Database from 'better-sqlite3'        // Node.js

const db = new WasmSqlite()
await db.init()

const sink = new MzSqliteSink({
  db,
  schema: 'server',  // or 'infer' or { columns: {...}, primaryKey: 'id' }
  serverBaseUrl: 'http://localhost:8080',
})

const client = new MzSubscribeClient({
  transport: new MzHttpTransport({ baseUrl: 'http://localhost:8080' }),
})
client.addSink(sink)
client.addView('live_orders')

// Query the local replica
const rows = await sink.query('SELECT * FROM live_orders WHERE status = ?', ['placed'])
```

## Schema Discovery

Three modes for creating SQLite tables:

### Server (recommended)
Fetches column names and types from `/api/views/:name/schema`:
```typescript
new MzSqliteSink({ db, schema: 'server', serverBaseUrl: 'http://localhost:8080' })
```

### Explicit
Client-defined columns:
```typescript
new MzSqliteSink({
  db,
  schema: {
    columns: { id: 'integer', name: 'text', total: 'real' },
    primaryKey: 'id',
  },
})
```

### Infer (default)
Creates the table from the first row of data:
```typescript
new MzSqliteSink({ db })  // schema defaults to 'infer'
```

## SqliteExec Interface

Any SQLite runtime can be used — just implement this interface:

```typescript
interface SqliteExec {
  exec(sql: string): Promise<void>
  query<T>(sql: string, params?: unknown[]): Promise<T[]>
}
```

Built-in implementations:
- `@mz-web/sqlite-wasm` — wa-sqlite in a Web Worker (browser)

Compatible with wrapper around:
- `better-sqlite3` (Node.js)
- Bun's built-in SQLite
- Deno's `@db/sqlite`

## API

### MzSqliteSink

| Method | Description |
|--------|-------------|
| `onBatch(batch)` | MzSink interface — called by subscribe client |
| `query(sql, params?)` | Run a SELECT query |
| `subscribeTable(view, cb)` | Subscribe to commits on a table |
| `subscribe(cb)` | Subscribe to all commits |
| `getDiagnostics()` | Table row counts, write/commit/query stats |

### Properties

| Property | Description |
|----------|-------------|
| `version` | Monotonic commit counter |
| `lastCommitTime` | Wall-clock ms of last commit |
| `totalWrites` | Cumulative row writes |
| `totalCommits` | Cumulative transactions |
| `totalQueries` | Cumulative SELECT queries |

### SQL Utilities (exported)

| Function | Description |
|----------|-------------|
| `escapeVal(v)` | Escape a value for inline SQL |
| `createTableDDL(view, schema)` | Generate CREATE TABLE DDL |
| `batchWriteSQL(view, schema, upserts, deletes)` | Generate batch INSERT/DELETE SQL |
| `pgTypeToSqlite(pgType)` | Map PG type to SQLite type |
| `inferSchema(row, pk?)` | Infer schema from a JS object |
| `fetchServerSchema(baseUrl, view)` | Fetch schema from mz-http |
