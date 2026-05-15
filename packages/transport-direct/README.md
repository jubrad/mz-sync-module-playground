# @mz-web/transport-direct

Direct WebSocket transport for Materialize — connects to Materialize's native WebSocket API without needing an mz-http proxy.

## Install

```bash
npm install @mz-web/transport-direct @mz-web/subscribe
```

## Usage

```typescript
import { MzSubscribeClient } from '@mz-web/subscribe'
import { MzDirectTransport } from '@mz-web/transport-direct'

const transport = new MzDirectTransport({
  host: 'abc123.us-east-1.aws.materialize.cloud',
  auth: { user: 'you@example.com', password: 'mzp_...' },
  cluster: 'default',
})

// Register views with their SUBSCRIBE SQL and key column
transport
  .registerView('live_orders', {
    sql: 'SELECT * FROM live_orders',
    keyColumn: 'id',
  })
  .registerView('store_dashboard', {
    sql: 'SELECT * FROM store_dashboard',
    keyColumn: 'id',
  })

const client = new MzSubscribeClient({ transport })
client.addView('live_orders')
client.addView('store_dashboard')
```

## How It Works

Each `connect()` call:

1. Opens a WebSocket to `wss://<host>/api/experimental/sql`
2. Authenticates with user/password or token
3. Sends `SUBSCRIBE (<sql>) WITH (SNAPSHOT, PROGRESS) ENVELOPE UPSERT (KEY ("<keyColumn>"))`
4. Parses Row messages into `MzEvent` objects (upsert/delete/progress)
5. Yields them as an `AsyncIterable`

On reconnect (handled by the subscribe client), if the view has a progress timestamp, it sends `AS OF <timestamp>` to resume from where it left off.

## Options

```typescript
new MzDirectTransport({
  host: string         // MZ host (no protocol prefix)
  auth: MzAuth         // { user, password } or { token }
  tls?: boolean        // Default: true (wss://)
  cluster?: string     // SET CLUSTER before SUBSCRIBE
  database?: string    // Default: "materialize"
})
```

## View Registration

Unlike `@mz-web/transport-http` (where the server knows the view SQL), the direct transport needs to know what SQL to SUBSCRIBE to:

```typescript
transport.registerView('my_view', {
  sql: 'SELECT * FROM my_materialized_view',
  keyColumn: 'id',
})

// Or with arbitrary SQL:
transport.registerView('filtered', {
  sql: 'SELECT * FROM orders WHERE status != \'delivered\'',
  keyColumn: 'order_id',
})
```

## Comparison with @mz-web/transport-http

| | transport-http | transport-direct |
|---|---|---|
| Connects to | mz-http proxy server | Materialize directly |
| Auth | None (trusts proxy) | User/password or token |
| SQL | Server-configured | Client-provided |
| CORS | Works in browser | May be blocked by MZ CORS |
| Best for | Browser apps | Node.js, edge, server-side |

## Authentication

**User/password (Materialize Cloud):**
```typescript
{ user: 'you@example.com', password: 'mzp_abc123...' }
```

**Token-based:**
```typescript
{ token: 'your-access-token' }
```
