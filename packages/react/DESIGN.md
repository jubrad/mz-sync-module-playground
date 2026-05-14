# @mz-web/react — Architecture

## Overview

`@mz-web/react` provides the React binding layer for the Materialize subscribe ecosystem. It connects React components to live-updating Materialize views through a local SQLite database that acts as a read-optimized cache.

## Data Flow

```
Materialize DB
    |
    v
mz-http server (SSE/WebSocket)
    |
    v
MzSubscribeClient (@mz-web/subscribe)
    |  - manages connections to views
    |  - batches events between progress boundaries
    |
    v
MzSqliteSink (@mz-web/sink-sqlite)
    |  - writes batches to local SQLite (WASM)
    |  - notifies table-level subscribers on commit
    |
    v
React hooks (@mz-web/react)
    |  - useLiveQuery: subscribes to table commits, re-runs SQL
    |  - useFreshness: samples lastCommitTime for staleness
    |
    v
React components (re-render with fresh data)
```

## Hook Architecture

### MzProvider / useMzContext

The provider injects three values into the React tree:

- **baseUrl** — The mz-http server URL, used by REST-based hooks (`useMzViews`, `useMzKey`).
- **client** — The `MzSubscribeClient` instance, exposed for advanced use cases (adding/removing views, accessing view handles).
- **sqliteSink** — The `MzSqliteSink` instance, used by query hooks to run SQL and subscribe to commits.

### useLiveQuery

This is the primary hook. It bridges the gap between the push-based sink (which receives batches from Materialize) and pull-based React rendering (which needs rows from SQL queries).

**Design decisions:**

1. **useState + useCallback, not useSyncExternalStore** — Queries against SQLite are async (the WASM layer uses message passing to a worker). `useSyncExternalStore` requires a synchronous `getSnapshot`, which does not work here. Instead, we use a subscription pattern where table commits trigger an async re-query.

2. **Table-level granularity** — The hook subscribes to specific table names via `MzSqliteSink.subscribeTable()`. This means a query joining `orders` and `customers` can subscribe to both tables and re-run when either receives new data.

3. **Null SQL for conditional queries** — Passing `null` as the SQL string disables the hook entirely, returning empty rows. This is the standard React pattern for conditional data fetching.

4. **Version counter** — The `version` field increments on every successful query, allowing components to detect data changes even when the row set is referentially different but semantically identical.

### useLiveQueryScalar

A thin wrapper that extracts the first column of the first row. Implemented via `useMemo` over the `useLiveQuery` result to avoid redundant computation.

### useMzViews

A polling hook that hits the REST API. It does not use the subscribe/sink path because view metadata (hydration status, progress timestamps) is server-side state that does not flow through the SUBSCRIBE protocol.

### useMzKey

A simple REST fetch hook for looking up individual keys. Useful for detail views or when the full subscribe pipeline is not needed for a single record.

### useFreshness

Measures the "staleness" of local data by computing how long ago the last commit was received. Uses a rolling window of 15 samples (200ms interval over 3 seconds) to smooth out jitter.

The color thresholds are:
- **Green** (<500ms): Data is essentially real-time
- **Yellow** (<2000ms): Noticeable lag, but within acceptable bounds
- **Red** (>=2000ms): Significant staleness, may indicate connection issues

## Connection to Sinks

The hooks do not own the sink lifecycle. The application is responsible for:

1. Creating the `MzSubscribeClient` and `MzSqliteSink`
2. Wiring them together via `client.addSink(sink)`
3. Passing both into `MzProvider`

This separation keeps the hooks focused on React concerns (state, effects, subscriptions) while the sink/client layer handles transport, batching, and persistence.

## Why Local SQLite?

Running SQL queries against a local SQLite (via WASM) has several advantages over keeping data in JavaScript objects:

- **Joins and aggregations** — Components can run arbitrary SQL, including joins across multiple Materialize views.
- **Indexing** — SQLite's query planner handles filtering and sorting efficiently.
- **Memory efficiency** — Large datasets live in WASM linear memory, not on the JS heap.
- **Consistency** — Batch writes are transactional; queries always see a consistent snapshot.

## Testing Strategy

The test suite verifies that all hooks can be imported and are properly typed. Integration testing with actual React rendering and mock sinks is recommended at the application level using `@testing-library/react` with a mock `MzProvider`.
