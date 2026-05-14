# @mz-web/sink-tanstack — Design

## Overview

This package bridges Materialize's SUBSCRIBE stream (via `@mz-web/subscribe`) and TanStack DB's collection sync API. It translates `MzBatch` objects into TanStack DB transactions so that collections stay in sync with Materialize views in real time.

## Architecture

```
  Materialize SUBSCRIBE
        |
        v
  MzSubscribeClient          (from @mz-web/subscribe)
        |
        | onBatch(MzBatch)
        v
  mzCollectionSync sink      (this package)
        |
        | begin / write / commit / markReady
        v
  TanStack DB Collection
        |
        v
  React components (via useQuery)
```

The package exposes a single factory function, `mzCollectionSync(client, viewName)`, which returns a `{ sync }` object. The `sync` function follows TanStack DB's sync protocol: it receives `SyncParams` and returns a cleanup function.

## Design decisions

### 1. Sync function pattern (not a class)

TanStack DB's `createCollection` expects a `sync` property that is a function. Rather than creating a class that implements `MzSink` and then adapting it, we create the sink inline inside the sync function. This keeps the API surface minimal and avoids lifecycle coordination issues.

### 2. View filtering inside the sink

The subscribe client broadcasts every batch to every registered sink. Our sink checks `batch.view === viewName` and ignores non-matching batches. This is simple and correct. If performance becomes a concern with many views, the subscribe client could be extended with per-view sink registration, but the current approach keeps the coupling minimal.

### 3. Inline TanStack DB types

We type the TanStack DB interfaces (`SyncParams`, `SyncFn`) inline rather than importing from `@tanstack/db`. This means:

- The package compiles without `@tanstack/db` installed (it's a peer dep)
- No version coupling to TanStack DB's internal type surface
- Users get type-safety at the boundary without transitive type dependencies

If TanStack DB's sync protocol changes, only these inline types need updating.

### 4. Upserts map to `insert` (not `update`)

TanStack DB's sync API uses `insert` for "this row should exist with this value" — it's an upsert semantic internally. We use `type: 'insert'` for all Materialize upserts rather than trying to distinguish inserts from updates, because:

- Materialize's SUBSCRIBE doesn't distinguish between initial inserts and updates
- TanStack DB's insert operation is idempotent when keyed by primary key

### 5. Deletes use the primary key only

When mapping `MzBatch.deletes` (a `Set<string>` of keys), we construct a minimal object `{ [pk]: key }`. TanStack DB uses this to identify and remove the row. The `pk` option defaults to `"id"` which matches the most common convention.

### 6. markReady on snapshot batches only

`markReady()` signals to TanStack DB that the initial dataset is loaded and the collection is ready for queries. We call it only after `kind: 'snapshot'` batches, which correspond to Materialize's `snapshot_complete` event. This ensures:

- The collection doesn't show partial data during initial load
- Subsequent `progress` and `catchup` batches don't re-trigger readiness

### 7. Empty batch skip

If a batch has zero upserts and zero deletes, we skip it entirely (no begin/commit). This avoids empty transactions in TanStack DB.

## Batch-to-transaction mapping

```
MzBatch {
  view: "orders",
  upserts: Map { "1" => {id:"1", product:"Widget"}, "2" => {id:"2", product:"Gadget"} },
  deletes: Set { "3" },
  progress: 1717200000000,
  kind: "snapshot"
}

  =>

begin()
write({ type: "insert", value: { id: "1", product: "Widget" } })
write({ type: "insert", value: { id: "2", product: "Gadget" } })
write({ type: "delete", value: { id: "3" } })
commit()
markReady()
```

## Future considerations

- **Optimistic updates:** TanStack DB supports optimistic mutations. A future version could coordinate with Materialize's write path to provide optimistic-then-confirmed updates.
- **Per-view sink registration:** If many collections are active, filtering in each sink adds O(views) overhead per batch. The subscribe client could support targeted delivery.
- **Conflict resolution:** TanStack DB has conflict resolution hooks. Materialize's SUBSCRIBE is authoritative (server wins), so no conflict resolution is needed today, but hybrid local-first patterns might benefit from it.
