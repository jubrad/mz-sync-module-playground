# @mz-web/transport-http — Design

## Purpose

Implements `MzTransportProvider` from `@mz-web/subscribe` for the mz-http server's WebSocket and SSE APIs. This is the default transport for browser-based applications.

## Architecture

```
MzHttpTransport.connect(view, opts)
    │
    ├── mode='ws'  → createWsStream()  → WebSocket → AsyncQueue → MzViewStream
    │
    └── mode='sse' → createSseStream() → EventSource → AsyncQueue → MzViewStream
```

## AsyncQueue Pattern

Browser APIs (WebSocket, EventSource) are push-based (callbacks). The subscribe client expects pull-based (AsyncIterable). `AsyncQueue` bridges this:

```
ws.onmessage(data)  →  queue.push(parsed)  →  for await (event of queue) { ... }
ws.onclose()        →  queue.end()         →  loop terminates
```

The queue handles:
- **Buffering**: if items are pushed faster than consumed, they queue up
- **Blocking**: if the consumer is faster, it awaits the next push
- **Termination**: `end()` causes the async iterator to return `done: true`

## URL Construction

WebSocket URLs are derived from the base URL:
- `http://` → `ws://`, `https://` → `wss://`
- Empty base URL → derives from `window.location`
- Path: `/api/ws/:view?since=X` or `?snapshot=true`

SSE URLs use the base URL directly:
- Path: `/api/sse/:view?since=X` or `?snapshot=true`

## Value Parsing

The transport parses `MzEvent.value` from string to object when possible. The mz-http server sends values as JSON strings (from Materialize's `jsonb_build_object()::text`). The transport attempts `JSON.parse()` on upsert values, falling back to the raw string on failure.

## What This Package Does NOT Do

- **Reconnection** — the subscribe client handles this
- **Progress tracking** — the subscribe client handles this
- **Event batching** — the subscribe client handles this
- **Authentication** — not implemented yet (would be added as headers/params)

## Future Transports

The same `MzTransportProvider` interface can be implemented for:
- **Direct Materialize WebSocket** — `@mz-web/transport-direct`, connecting to MZ's native WS subscribe API
- **gRPC** — if MZ adds a gRPC subscribe API
- **Mock** — for testing (just an async generator)
