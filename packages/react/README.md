# @mz-web/react

React hooks for the Materialize subscribe ecosystem. Provides reactive data binding between Materialize views and React components via a local SQLite sink.

## Installation

```bash
npm install @mz-web/react react
```

## Setup

Wrap your app in `MzProvider` with a configured subscribe client and SQLite sink:

```tsx
import { MzProvider } from '@mz-web/react'
import { MzSubscribeClient } from '@mz-web/subscribe'
import { MzSqliteSink } from '@mz-web/sink-sqlite'

const client = new MzSubscribeClient({
  transport: myTransport,
  views: ['orders', 'customers'],
})

const sink = new MzSqliteSink({ db: myDb })
client.addSink(sink)

function App() {
  return (
    <MzProvider baseUrl="http://localhost:8000" client={client} sqliteSink={sink}>
      <Dashboard />
    </MzProvider>
  )
}
```

## Hooks

### `useLiveQuery<T>(sql, params?, tables?)`

Run a SQL query against the local SQLite sink. The query re-executes automatically whenever any of the listed `tables` receive new data from Materialize.

```tsx
import { useLiveQuery } from '@mz-web/react'

function OrderList() {
  const { rows, version } = useLiveQuery<{ id: string; total: number }>(
    'SELECT * FROM orders WHERE total > ? ORDER BY total DESC',
    [100],
    ['orders'],
  )

  return (
    <ul>
      {rows.map((o) => (
        <li key={o.id}>Order {o.id}: ${o.total}</li>
      ))}
    </ul>
  )
}
```

Pass `null` as the sql argument to disable the query:

```tsx
const { rows } = useLiveQuery(
  selectedTable ? `SELECT * FROM "${selectedTable}"` : null,
  [],
  selectedTable ? [selectedTable] : [],
)
```

### `useLiveQueryScalar<T>(sql, params?, tables?)`

Convenience wrapper that extracts the first column of the first row. Ideal for aggregate queries.

```tsx
import { useLiveQueryScalar } from '@mz-web/react'

function OrderCount() {
  const { value } = useLiveQueryScalar<number>(
    'SELECT count(*) FROM orders',
    [],
    ['orders'],
  )

  return <span>{value ?? 0} orders</span>
}
```

### `useMzViews(intervalMs?)`

Polls the mz-http server for view metadata at a regular interval (default 3000ms).

```tsx
import { useMzViews } from '@mz-web/react'

function ViewsList() {
  const { views, loading, latencyMs } = useMzViews(5000)

  if (loading) return <p>Loading...</p>

  return (
    <div>
      <p>Latency: {latencyMs}ms</p>
      <ul>
        {views.map((v) => (
          <li key={v.name}>
            {v.name} — {v.hydrated ? 'hydrated' : 'hydrating...'}
          </li>
        ))}
      </ul>
    </div>
  )
}
```

### `useMzKey<T>(viewName, key)`

Fetch a single key from a view via the REST API.

```tsx
import { useMzKey } from '@mz-web/react'

function OrderDetail({ orderId }: { orderId: string }) {
  const { value, loading, refetch } = useMzKey<{ id: string; total: number }>(
    'orders',
    orderId,
  )

  if (loading) return <p>Loading...</p>
  if (!value) return <p>Not found</p>

  return (
    <div>
      <h2>Order {value.id}</h2>
      <p>Total: ${value.total}</p>
      <button onClick={refetch}>Refresh</button>
    </div>
  )
}
```

### `useFreshness()`

Monitors the staleness of the local SQLite data by sampling `lastCommitTime` every 200ms and computing a rolling 3-second average.

```tsx
import { useFreshness } from '@mz-web/react'

function FreshnessIndicator() {
  const { avgMs, color, label } = useFreshness()

  return (
    <div style={{ color }}>
      {label} ({avgMs}ms)
    </div>
  )
}
```

### `useMzContext()`

Access the raw context value. Throws if called outside `MzProvider`.

```tsx
import { useMzContext } from '@mz-web/react'

function DebugPanel() {
  const { client, sqliteSink, baseUrl } = useMzContext()
  // Use client/sink directly for advanced scenarios
}
```

## Types

```ts
interface ViewInfo {
  name: string
  keys: string[]
  hydrated: boolean
  progress_timestamp: number
  title_columns: string[]
}

interface ViewsResponse {
  views: ViewInfo[]
  server_time_ms: number
}
```
