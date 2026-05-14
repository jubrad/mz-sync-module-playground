# @mz-web/sink-sqlite — Design

## Purpose

Writes Materialize SUBSCRIBE batches to any SQLite database. Decouples the SQL write logic from the specific SQLite runtime (WASM, native, etc.).

## Architecture

```
MzSubscribeClient                MzSqliteSink                 SqliteExec
─────────────────                ────────────                 ──────────
  delivers MzBatch ──────────►  schema resolution             any SQLite
                                table creation                   │
                                SQL generation    ──exec()──►    │
                                notification      ◄─────────    │
                                    │                           │
                                    ▼                           │
                              subscribers (React hooks)         │
```

## Key Design Decisions

### 1. SqliteExec as the Abstraction

The sink writes SQL strings — it doesn't use prepared statements, parameter binding, or any SQLite-specific API beyond `exec(sql)` and `query(sql, params)`. This makes it compatible with any SQLite runtime:

- **wa-sqlite** (WASM in Web Worker) — `@mz-web/sqlite-wasm`
- **better-sqlite3** (Node.js native) — wrap `db.exec()` and `db.prepare().all()`
- **Bun's SQLite** — wrap `db.run()` and `db.query().all()`
- **D1** (Cloudflare) — wrap `db.exec()` and `db.prepare().all()`

### 2. Inline SQL Over Prepared Statements

Batch writes use `escapeVal()` for inline value escaping rather than parameter binding. This is because:
- `exec(sql)` is the simplest interface — just a string in, nothing out
- Prepared statements behave differently across SQLite runtimes (especially async WASM)
- The performance difference is negligible for our batch sizes (each batch is one `BEGIN...COMMIT`)
- The trade-off is acceptable: we control the input (Materialize values), and `escapeVal()` handles the escaping

### 3. Lazy Table Creation

Tables are created on the first batch, not at sink construction time. This allows:
- Schema inference from the first data row (no upfront config needed)
- Server schema to be fetched only when data arrives (view may not exist yet)
- Multiple views to share one sink without pre-declaring all tables

### 4. Notification System

The sink has a subscriber system (subscribeTable, subscribe) separate from the subscribe client's sink interface. This is because React hooks need to be notified when data changes in a specific table, not when any batch arrives.

## SQL Generation

All SQL is generated as plain strings by pure functions in `table-writer.ts`. This makes them easy to test and inspect:

```typescript
createTableDDL('orders', schema)
// → CREATE TABLE IF NOT EXISTS "orders" ("id" INTEGER, "name" TEXT, PRIMARY KEY ("id"))

batchWriteSQL('orders', schema, upserts, deletes)
// → BEGIN;\nINSERT OR REPLACE INTO "orders" ("id","name") VALUES (1,'alice');\nCOMMIT
```

## Error Handling

If a batch write fails:
1. Attempt `ROLLBACK` to clean up the transaction
2. Log the error
3. Skip this batch (don't crash the sink)
4. The next batch will retry writes naturally

This is safe because Materialize's SUBSCRIBE guarantees monotonic progress — skipping a batch means those changes will appear in a subsequent batch.
