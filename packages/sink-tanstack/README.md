# @mz-web/sink-tanstack

TanStack DB sink for [`@mz-web/subscribe`](../subscribe/) — maps Materialize SUBSCRIBE batches to TanStack DB collection syncs.

## Installation

```bash
npm install @mz-web/sink-tanstack @mz-web/subscribe @tanstack/db
```

## Usage

`mzCollectionSync` creates a TanStack DB sync config that plugs directly into `createCollection()`. Each Materialize batch becomes an atomic TanStack DB transaction.

```ts
import { MzSubscribeClient } from '@mz-web/subscribe'
import { createCollection } from '@tanstack/db'
import { mzCollectionSync } from '@mz-web/sink-tanstack'

// 1. Set up the subscribe client
const client = new MzSubscribeClient({
  transport: myTransport,
  views: ['orders', 'inventory'],
})

// 2. Create TanStack DB collections backed by Materialize views
const orders = createCollection({
  id: 'orders',
  ...mzCollectionSync(client, 'orders'),
})

const inventory = createCollection({
  id: 'inventory',
  ...mzCollectionSync(client, 'inventory', { pk: 'sku' }),
})
```

### Custom primary key

By default, deletes are mapped using `id` as the primary key field. Pass `pk` to change this:

```ts
const users = createCollection({
  id: 'users',
  ...mzCollectionSync(client, 'users', { pk: 'user_id' }),
})
```

### With React

TanStack DB collections work with React out of the box. Once synced, your components re-render automatically when Materialize pushes new data:

```tsx
import { useQuery } from '@tanstack/db'

function OrderList() {
  const orders = useQuery(ordersCollection, {
    where: { status: 'pending' },
    orderBy: { created_at: 'desc' },
  })

  return (
    <ul>
      {orders.map(order => (
        <li key={order.id}>{order.product} x{order.qty}</li>
      ))}
    </ul>
  )
}
```

## API

### `mzCollectionSync(client, viewName, opts?)`

Creates a sync config object (`{ sync }`) for use with TanStack DB's `createCollection()`.

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `client` | `MzSubscribeClient` | The subscribe client instance |
| `viewName` | `string` | Name of the Materialize view to sync |
| `opts.pk` | `string` | Primary key field name (default: `"id"`) |

**Returns:** `{ sync: SyncFn<T> }` — spread this into your collection config.

### Batch mapping

| MzBatch field | TanStack DB operation |
|---|---|
| `upserts` (Map entries) | `write({ type: 'insert', value })` |
| `deletes` (Set entries) | `write({ type: 'delete', value: { [pk]: key } })` |
| `kind: 'snapshot'` | `markReady()` after commit |

Each batch is wrapped in `begin()` / `commit()` so TanStack DB sees it as one atomic transaction.

## License

MIT
