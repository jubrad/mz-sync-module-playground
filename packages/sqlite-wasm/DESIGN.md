# @mz-web/sqlite-wasm — Design

## Purpose

Provides the `SqliteExec` interface for browsers using wa-sqlite compiled to WebAssembly, running in a dedicated Web Worker to avoid blocking the main thread.

## Architecture

The only package that touches browser Worker APIs. All other packages are runtime-agnostic.

```
@mz-web/sink-sqlite          @mz-web/sqlite-wasm
────────────────────          ──────────────────
MzSqliteSink                  WasmSqlite (main thread)
  calls exec(sql) ──────────► postMessage({type:'exec', sql})
  calls query(sql) ─────────► postMessage({type:'query', sql})
                                    │
                               Web Worker
                               ┌──────────────┐
                               │ wa-sqlite     │
                               │ :memory: DB   │
                               │ PRAGMA WAL    │
                               │ synchronous=  │
                               │   OFF         │
                               └──────────────┘
```

## Key Design Decisions

### 1. Dumb Worker

The worker knows nothing about tables, schemas, views, or Materialize. It receives SQL strings and executes them. This keeps the worker ~60 lines and makes it easy to replace with a different SQLite runtime.

### 2. Promise-per-Message

Each `exec()`/`query()` call generates a unique ID, sends a postMessage, and returns a promise that resolves when the worker responds with that ID. This serializes access naturally — messages are processed in order.

### 3. Worker URL as Constructor Param

Different bundlers handle Web Workers differently (Vite's `?worker&url`, webpack's `new URL(...)`, manual URL). Rather than coupling to one bundler, the `workerUrl` is a required constructor param. The consumer provides the URL appropriate for their bundler.

### 4. Graceful Error Handling

- `query()` returns `[]` before init (not an error)
- Worker `no such table` errors resolve to `[]` (not reject) — prevents hooks from crashing during startup race
- Other errors reject normally
- `destroy()` rejects all pending promises

## Message Protocol

| Direction | Type | Fields | Response |
|-----------|------|--------|----------|
| → Worker | init | id | ready (id) |
| → Worker | exec | id, sql | done (id) |
| → Worker | query | id, sql, params? | result (id, rows) |
| ← Main | ready | id | — |
| ← Main | done | id | — |
| ← Main | result | id, rows | — |
| ← Main | error | id, error | — |
