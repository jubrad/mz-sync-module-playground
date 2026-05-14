# @mz-web/subscribe — Design

## Purpose

Canonical JavaScript client for Materialize SUBSCRIBE. Decouples the subscribe protocol (progress tracking, batching, reconnection) from both the wire transport and the storage backend.

## Architecture

```
Transport Provider (pluggable)        Subscribe Client            Sinks (pluggable)
─────────────────────────────        ────────────────            ──────────────────
MzHttpTransport                      MzSubscribeClient           MzSqliteSink
MzDirectTransport (future)              │                        MzTanStackSink
                                        │ consumes               Custom sinks
    ┌─────────────┐                     │ AsyncIterable
    │ WebSocket   │──► MzViewStream ──► │ BatchAccumulator
    │ SSE         │    (events)         │   ├─ upsert/delete → accumulate
    │ Native WS   │                     │   └─ progress → seal batch
    └─────────────┘                     │
                                        │──► MzBatch ──► sink.onBatch()
                                        │
                                        │ reconnect on disconnect
                                        │ exponential backoff
                                        │ since/snapshot decisions
```

## Key Design Decisions

### 1. AsyncIterable over Callbacks

The transport interface uses `AsyncIterable<MzEvent>` instead of callbacks because:
- Natural backpressure: if the consumer is slow, the iterator blocks
- Clean lifecycle: `for await` terminates when the stream ends
- Composable: can use standard async iteration utilities
- Testable: mock transports are just async generators

### 2. Batch as the Unit of Work

Raw events are internal to the client. Sinks receive `MzBatch` objects — the collapsed, progress-sealed set of changes. This:
- Ensures consistency: sinks see complete progress boundaries
- Reduces work: collapsed batches have at most one entry per key
- Simplifies sinks: they receive a map of upserts + set of deletes

### 3. Reconnection in the Client, Not the Transport

The transport is a dumb pipe. It connects, yields events, and terminates. The subscribe client handles:
- When to reconnect (on stream termination)
- How long to wait (exponential backoff)
- What to send (since vs snapshot based on progress)

This keeps transports simple and testable.

### 4. Ref-counted Views

Multiple React components may subscribe to the same view. The client ref-counts `addView`/`removeView` calls and only tears down the transport when the count hits 0.

### 5. Multiple Sinks

A single client can feed multiple sinks simultaneously. This supports the hybrid model: SQLite for SQL queries + TanStack DB for reactive collections, both receiving the same batches.

## Event Flow

```
1. Transport yields MzEvent from wire
2. BatchAccumulator.add(event) — accumulates upserts/deletes
3. On progress event: BatchAccumulator.seal() → MzBatch
4. Client delivers MzBatch to all registered sinks
5. Sinks write to their storage (SQLite, TanStack DB, etc.)
6. On stream termination: schedule reconnect with backoff
7. On reconnect: use last progress timestamp for ?since= catchup
```

## Error Handling

- Transport errors terminate the async iterable → client reconnects
- Sink errors are caught and logged, don't affect other sinks
- Lagged events reset progress to 0 → full snapshot on reconnect
- The client never throws to callers — errors are handled internally

## Testing Strategy

- `BatchAccumulator`: pure unit tests on event collapsing and sealing
- `MzSubscribeClient`: integration tests with mock `MzTransportProvider` that yields canned event sequences
- No DOM/browser dependencies — runs in any JS environment
