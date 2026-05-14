# @mz-web/transport-http

WebSocket and SSE transport provider for `@mz-web/subscribe`. Connects to an [mz-http](https://github.com/your-org/mz-http) server.

## Install

```bash
npm install @mz-web/transport-http @mz-web/subscribe
```

## Usage

```typescript
import { MzSubscribeClient } from '@mz-web/subscribe'
import { MzHttpTransport } from '@mz-web/transport-http'

const client = new MzSubscribeClient({
  transport: new MzHttpTransport({
    baseUrl: 'http://localhost:8080',
    mode: 'ws',  // or 'sse'
  }),
})

client.addView('live_orders')
```

## Options

```typescript
new MzHttpTransport({
  baseUrl: string    // mz-http server URL. Empty string = same origin.
  mode?: 'ws' | 'sse'  // Default: 'ws'
})
```

## How It Works

Each `connect()` call opens a single WebSocket or SSE connection to mz-http:

- **WebSocket**: `ws://host/api/ws/:view?since=X` or `?snapshot=true`
- **SSE**: `http://host/api/sse/:view?since=X` or `?snapshot=true`

Messages are parsed from JSON into typed `MzEvent` objects and yielded as an `AsyncIterable`. When the connection closes, the iterable terminates — the subscribe client handles reconnection.

## AsyncQueue

Also exports `AsyncQueue<T>` — a utility that converts push-based APIs (WebSocket.onmessage) into async iterables. Useful for building custom transports:

```typescript
import { AsyncQueue } from '@mz-web/transport-http'

const queue = new AsyncQueue<MyEvent>()
ws.onmessage = (e) => queue.push(JSON.parse(e.data))
ws.onclose = () => queue.end()

for await (const event of queue) {
  // consume events
}
```
