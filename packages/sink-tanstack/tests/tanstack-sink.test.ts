import { describe, it, expect, vi, beforeEach } from 'vitest'
import type { MzBatch, MzSink } from '@mz-web/subscribe'
import { mzCollectionSync } from '../src/tanstack-sink.js'
import type { SyncParams } from '../src/tanstack-sink.js'

// ── Mock subscribe client ───────────────────────────────

function createMockClient() {
  const sinks = new Set<MzSink>()
  return {
    addSink(sink: MzSink) {
      sinks.add(sink)
      return () => { sinks.delete(sink) }
    },
    /** Deliver a batch to all registered sinks. */
    async deliver(batch: MzBatch) {
      for (const sink of sinks) {
        await sink.onBatch(batch)
      }
    },
    /** Notify all sinks of a connection state change. */
    notifyConnectionChange(view: string, connected: boolean) {
      for (const sink of sinks) {
        sink.onConnectionChange?.(view, connected)
      }
    },
    get sinkCount() { return sinks.size },
  }
}

// ── Mock TanStack DB sync params ────────────────────────

function createMockSyncParams<T = Record<string, unknown>>() {
  const calls: Array<{ method: string; args?: unknown[] }> = []
  const params: SyncParams<T> = {
    begin: vi.fn(() => calls.push({ method: 'begin' })),
    write: vi.fn((op) => calls.push({ method: 'write', args: [op] })),
    commit: vi.fn(() => calls.push({ method: 'commit' })),
    markReady: vi.fn(() => calls.push({ method: 'markReady' })),
    collection: {},
  }
  return { params, calls }
}

// ── Helpers ─────────────────────────────────────────────

function makeBatch(overrides: Partial<MzBatch> = {}): MzBatch {
  return {
    view: 'orders',
    upserts: new Map(),
    deletes: new Set(),
    progress: 1000,
    kind: 'progress',
    ...overrides,
  }
}

// ── Tests ───────────────────────────────────────────────

describe('mzCollectionSync', () => {
  let client: ReturnType<typeof createMockClient>

  beforeEach(() => {
    client = createMockClient()
  })

  it('registers a sink on the subscribe client', () => {
    const { sync } = mzCollectionSync(client as any, 'orders')
    const { params } = createMockSyncParams()
    const cleanup = sync(params)
    expect(client.sinkCount).toBe(1)
    cleanup()
    expect(client.sinkCount).toBe(0)
  })

  it('maps upserts to insert writes', async () => {
    const { sync } = mzCollectionSync(client as any, 'orders')
    const { params, calls } = createMockSyncParams()
    sync(params)

    await client.deliver(makeBatch({
      upserts: new Map([
        ['1', { id: '1', product: 'Widget', qty: 5 }],
        ['2', { id: '2', product: 'Gadget', qty: 3 }],
      ]),
    }))

    expect(calls).toEqual([
      { method: 'begin' },
      { method: 'write', args: [{ type: 'insert', value: { id: '1', product: 'Widget', qty: 5 } }] },
      { method: 'write', args: [{ type: 'insert', value: { id: '2', product: 'Gadget', qty: 3 } }] },
      { method: 'commit' },
    ])
  })

  it('maps deletes to delete writes with pk', async () => {
    const { sync } = mzCollectionSync(client as any, 'orders')
    const { params, calls } = createMockSyncParams()
    sync(params)

    await client.deliver(makeBatch({
      deletes: new Set(['42', '99']),
    }))

    expect(calls).toEqual([
      { method: 'begin' },
      { method: 'write', args: [{ type: 'delete', value: { id: '42' } }] },
      { method: 'write', args: [{ type: 'delete', value: { id: '99' } }] },
      { method: 'commit' },
    ])
  })

  it('uses custom pk option', async () => {
    const { sync } = mzCollectionSync(client as any, 'orders', { pk: 'order_id' })
    const { params, calls } = createMockSyncParams()
    sync(params)

    await client.deliver(makeBatch({
      deletes: new Set(['42']),
    }))

    expect(calls).toEqual([
      { method: 'begin' },
      { method: 'write', args: [{ type: 'delete', value: { order_id: '42' } }] },
      { method: 'commit' },
    ])
  })

  it('handles mixed upserts and deletes in one batch', async () => {
    const { sync } = mzCollectionSync(client as any, 'orders')
    const { params, calls } = createMockSyncParams()
    sync(params)

    await client.deliver(makeBatch({
      upserts: new Map([['1', { id: '1', product: 'Widget', qty: 10 }]]),
      deletes: new Set(['2']),
    }))

    expect(calls).toEqual([
      { method: 'begin' },
      { method: 'write', args: [{ type: 'insert', value: { id: '1', product: 'Widget', qty: 10 } }] },
      { method: 'write', args: [{ type: 'delete', value: { id: '2' } }] },
      { method: 'commit' },
    ])
  })

  it('calls markReady on snapshot batch', async () => {
    const { sync } = mzCollectionSync(client as any, 'orders')
    const { params, calls } = createMockSyncParams()
    sync(params)

    await client.deliver(makeBatch({
      kind: 'snapshot',
      upserts: new Map([['1', { id: '1', product: 'Widget', qty: 5 }]]),
    }))

    expect(calls).toEqual([
      { method: 'markReady' },
      { method: 'begin' },
      { method: 'write', args: [{ type: 'insert', value: { id: '1', product: 'Widget', qty: 5 } }] },
      { method: 'commit' },
    ])
  })

  it('does NOT call markReady on progress batch', async () => {
    const { sync } = mzCollectionSync(client as any, 'orders')
    const { params } = createMockSyncParams()
    sync(params)

    await client.deliver(makeBatch({
      kind: 'progress',
      upserts: new Map([['1', { id: '1', product: 'Widget', qty: 5 }]]),
    }))

    expect(params.markReady).not.toHaveBeenCalled()
  })

  it('calls markReady on catchup batch', async () => {
    const { sync } = mzCollectionSync(client as any, 'orders')
    const { params } = createMockSyncParams()
    sync(params)

    await client.deliver(makeBatch({
      kind: 'catchup',
      upserts: new Map([['1', { id: '1', product: 'Widget', qty: 5 }]]),
    }))

    expect(params.markReady).toHaveBeenCalledTimes(1)
  })

  it('ignores batches for other views', async () => {
    const { sync } = mzCollectionSync(client as any, 'orders')
    const { params } = createMockSyncParams()
    sync(params)

    await client.deliver(makeBatch({
      view: 'inventory',
      upserts: new Map([['1', { id: '1', sku: 'ABC' }]]),
    }))

    expect(params.begin).not.toHaveBeenCalled()
    expect(params.write).not.toHaveBeenCalled()
    expect(params.commit).not.toHaveBeenCalled()
  })

  it('skips empty batches', async () => {
    const { sync } = mzCollectionSync(client as any, 'orders')
    const { params } = createMockSyncParams()
    sync(params)

    await client.deliver(makeBatch())

    expect(params.begin).not.toHaveBeenCalled()
  })

  it('cleanup function unregisters the sink', async () => {
    const { sync } = mzCollectionSync(client as any, 'orders')
    const { params } = createMockSyncParams()
    const cleanup = sync(params)

    cleanup()

    await client.deliver(makeBatch({
      upserts: new Map([['1', { id: '1', product: 'Widget', qty: 5 }]]),
    }))

    expect(params.begin).not.toHaveBeenCalled()
  })

  it('supports multiple collections for different views', async () => {
    const { sync: ordersSync } = mzCollectionSync(client as any, 'orders')
    const { sync: inventorySync } = mzCollectionSync(client as any, 'inventory')
    const orderParams = createMockSyncParams()
    const inventoryParams = createMockSyncParams()
    ordersSync(orderParams.params)
    inventorySync(inventoryParams.params)

    await client.deliver(makeBatch({
      view: 'orders',
      upserts: new Map([['1', { id: '1', product: 'Widget' }]]),
    }))

    await client.deliver(makeBatch({
      view: 'inventory',
      upserts: new Map([['a', { id: 'a', sku: 'SKU-1' }]]),
    }))

    expect(orderParams.params.begin).toHaveBeenCalledTimes(1)
    expect(inventoryParams.params.begin).toHaveBeenCalledTimes(1)
    expect(orderParams.calls[1]).toEqual({
      method: 'write',
      args: [{ type: 'insert', value: { id: '1', product: 'Widget' } }],
    })
    expect(inventoryParams.calls[1]).toEqual({
      method: 'write',
      args: [{ type: 'insert', value: { id: 'a', sku: 'SKU-1' } }],
    })
  })

  it('calls markReady for empty snapshot batches', async () => {
    const { sync } = mzCollectionSync(client as any, 'orders')
    const { params } = createMockSyncParams()
    sync(params)

    await client.deliver(makeBatch({
      kind: 'snapshot',
      upserts: new Map(),
      deletes: new Set(),
    }))

    expect(params.markReady).toHaveBeenCalledTimes(1)
    // begin/write/commit should NOT be called for empty batches
    expect(params.begin).not.toHaveBeenCalled()
  })

  it('calls markReady only once across multiple snapshot batches', async () => {
    const { sync } = mzCollectionSync(client as any, 'orders')
    const { params } = createMockSyncParams()
    sync(params)

    await client.deliver(makeBatch({
      kind: 'snapshot',
      upserts: new Map([['1', { id: '1', product: 'Widget' }]]),
    }))

    await client.deliver(makeBatch({
      kind: 'snapshot',
      upserts: new Map([['2', { id: '2', product: 'Gadget' }]]),
    }))

    expect(params.markReady).toHaveBeenCalledTimes(1)
  })

  it('propagates begin/write/commit errors', async () => {
    const { sync } = mzCollectionSync(client as any, 'orders')
    const { params } = createMockSyncParams()
    params.begin = vi.fn(() => { throw new Error('begin failed') })
    sync(params)

    await expect(
      client.deliver(makeBatch({
        upserts: new Map([['1', { id: '1', product: 'Widget' }]]),
      }))
    ).rejects.toThrow('begin failed')
  })

  it('invokes onConnectionChange callback', () => {
    const onConnectionChange = vi.fn()
    const { sync } = mzCollectionSync(client as any, 'orders', { onConnectionChange })
    const { params } = createMockSyncParams()
    sync(params)

    client.notifyConnectionChange('orders', true)

    expect(onConnectionChange).toHaveBeenCalledWith('orders', true)
  })
})
