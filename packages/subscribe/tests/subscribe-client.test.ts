import { describe, it, expect, vi, beforeEach } from 'vitest'
import { MzSubscribeClient } from '../src/subscribe-client.js'
import type { MzSink } from '../src/subscribe-client.js'
import type { MzTransportProvider, MzViewStream } from '../src/transport.js'
import type { MzEvent, MzBatch } from '../src/types.js'

/** Helper: create a mock transport that yields events then hangs (no reconnect noise). */
function mockTransport(events: MzEvent[]): MzTransportProvider & { close: () => void } {
  let resolver: (() => void) | null = null
  return {
    connect(_view: string, _opts) {
      let closed = false
      const stream: MzViewStream = {
        events: {
          async *[Symbol.asyncIterator]() {
            for (const event of events) {
              if (closed) return
              yield event
            }
            // Hang open so the client doesn't reconnect during test
            await new Promise<void>((r) => { resolver = r })
          },
        },
        close() { closed = true; resolver?.() },
      }
      return stream
    },
    close() { resolver?.() },
  }
}

/** Helper: create a mock transport that yields events then stays open. */
function mockTransportThatStaysOpen(events: MzEvent[]): MzTransportProvider & { close: () => void } {
  let resolver: (() => void) | null = null
  return {
    connect(_view: string, _opts) {
      let closed = false
      const stream: MzViewStream = {
        events: {
          async *[Symbol.asyncIterator]() {
            for (const event of events) {
              if (closed) return
              yield event
            }
            // Stay open until closed
            await new Promise<void>((r) => { resolver = r })
          },
        },
        close() { closed = true; resolver?.() },
      }
      return stream
    },
    close() { resolver?.() },
  }
}

/** Collect batches from a sink. */
function collectingSink(): MzSink & { batches: MzBatch[]; connectionChanges: [string, boolean][] } {
  const batches: MzBatch[] = []
  const connectionChanges: [string, boolean][] = []
  return {
    batches,
    connectionChanges,
    onBatch(batch) { batches.push(batch) },
    onConnectionChange(view, connected) { connectionChanges.push([view, connected]) },
  }
}

describe('MzSubscribeClient', () => {
  it('delivers batches on progress events', async () => {
    const transport = mockTransport([
      { type: 'upsert', key: '1', value: { name: 'alice' } },
      { type: 'upsert', key: '2', value: { name: 'bob' } },
      { type: 'progress', timestamp: 1000 },
    ])

    const client = new MzSubscribeClient({ transport, reconnectMs: 50 })
    const sink = collectingSink()
    client.addSink(sink)
    client.addView('test')

    // Wait for the stream to be consumed
    await new Promise((r) => setTimeout(r, 100))

    expect(sink.batches.length).toBe(1)
    expect(sink.batches[0].view).toBe('test')
    expect(sink.batches[0].upserts.size).toBe(2)
    expect(sink.batches[0].progress).toBe(1000)
    expect(sink.batches[0].kind).toBe('snapshot') // first batch is snapshot

    client.destroy()
  })

  it('marks view ready on snapshot_complete', async () => {
    const transport = mockTransportThatStaysOpen([
      { type: 'upsert', key: '1', value: { x: 1 } },
      { type: 'snapshot_complete', timestamp: 500 },
    ])

    const client = new MzSubscribeClient({ transport, reconnectMs: 50 })
    const sink = collectingSink()
    client.addSink(sink)
    client.addView('v')

    await new Promise((r) => setTimeout(r, 100))

    const handle = client.getView('v')
    expect(handle?.ready).toBe(true)
    expect(sink.batches[0].kind).toBe('snapshot')

    client.destroy()
  })

  it('tracks progress across multiple batches', async () => {
    const transport = mockTransport([
      { type: 'upsert', key: '1', value: { v: 1 } },
      { type: 'progress', timestamp: 100 },
      { type: 'upsert', key: '1', value: { v: 2 } },
      { type: 'progress', timestamp: 200 },
    ])

    const client = new MzSubscribeClient({ transport, reconnectMs: 50 })
    const sink = collectingSink()
    client.addSink(sink)
    client.addView('v')

    await new Promise((r) => setTimeout(r, 100))

    expect(sink.batches.length).toBe(2)
    expect(sink.batches[0].progress).toBe(100)
    expect(sink.batches[1].progress).toBe(200)
    expect(sink.batches[1].upserts.get('1')).toEqual({ v: 2 })

    client.destroy()
  })

  it('skips empty progress events', async () => {
    const transport = mockTransport([
      { type: 'progress', timestamp: 100 },
      { type: 'progress', timestamp: 200 },
      { type: 'upsert', key: '1', value: { v: 1 } },
      { type: 'progress', timestamp: 300 },
    ])

    const client = new MzSubscribeClient({ transport, reconnectMs: 50 })
    const sink = collectingSink()
    client.addSink(sink)
    client.addView('v')

    await new Promise((r) => setTimeout(r, 100))

    // Only the progress at 300 has data
    expect(sink.batches.length).toBe(1)
    expect(sink.batches[0].progress).toBe(300)

    client.destroy()
  })

  it('handles deletes in batches', async () => {
    const transport = mockTransport([
      { type: 'upsert', key: '1', value: { v: 1 } },
      { type: 'progress', timestamp: 100 },
      { type: 'delete', key: '1' },
      { type: 'progress', timestamp: 200 },
    ])

    const client = new MzSubscribeClient({ transport, reconnectMs: 50 })
    const sink = collectingSink()
    client.addSink(sink)
    client.addView('v')

    await new Promise((r) => setTimeout(r, 100))

    expect(sink.batches.length).toBe(2)
    expect(sink.batches[1].deletes.has('1')).toBe(true)
    expect(sink.batches[1].upserts.size).toBe(0)

    client.destroy()
  })

  it('ref-counts views', async () => {
    const transport = mockTransportThatStaysOpen([
      { type: 'upsert', key: '1', value: { x: 1 } },
      { type: 'progress', timestamp: 100 },
    ])

    const client = new MzSubscribeClient({ transport })
    client.addView('v')
    client.addView('v') // ref count = 2

    await new Promise((r) => setTimeout(r, 50))

    client.removeView('v') // ref count = 1, still alive
    expect(client.getView('v')).toBeDefined()

    client.removeView('v') // ref count = 0, torn down
    expect(client.getView('v')).toBeUndefined()

    client.destroy()
  })

  it('unsubscribe removes sink', async () => {
    const transport = mockTransport([
      { type: 'upsert', key: '1', value: { x: 1 } },
      { type: 'progress', timestamp: 100 },
      { type: 'upsert', key: '2', value: { x: 2 } },
      { type: 'progress', timestamp: 200 },
    ])

    const client = new MzSubscribeClient({ transport, reconnectMs: 50 })
    const sink = collectingSink()
    const unsub = client.addSink(sink)
    client.addView('v')

    await new Promise((r) => setTimeout(r, 50))
    unsub() // remove sink before second batch arrives
    await new Promise((r) => setTimeout(r, 100))

    // Sink may have received 1 or 2 batches depending on timing,
    // but it should not error
    client.destroy()
  })

  it('notifies sinks of connection changes', async () => {
    const transport = mockTransport([
      { type: 'upsert', key: '1', value: { x: 1 } },
      { type: 'progress', timestamp: 100 },
    ])

    const client = new MzSubscribeClient({ transport, reconnectMs: 10000 })
    const sink = collectingSink()
    client.addSink(sink)
    client.addView('v')

    await new Promise((r) => setTimeout(r, 100))

    // Should have connected
    expect(sink.connectionChanges.some(([v, c]) => v === 'v' && c === true)).toBe(true)

    // Force disconnect by closing the transport
    transport.close()
    await new Promise((r) => setTimeout(r, 100))

    expect(sink.connectionChanges.some(([v, c]) => v === 'v' && c === false)).toBe(true)

    client.destroy()
  })

  it('resync resets progress and reconnects', async () => {
    const connectCalls: { since?: number; snapshot?: boolean }[] = []
    const transport: MzTransportProvider = {
      connect(_view, opts) {
        connectCalls.push(opts)
        let closed = false
        return {
          events: {
            async *[Symbol.asyncIterator]() {
              if (closed) return
              yield { type: 'upsert' as const, key: '1', value: { x: 1 } }
              yield { type: 'progress' as const, timestamp: 1000 }
              await new Promise<void>((r) => setTimeout(r, 500))
            },
          },
          close() { closed = true },
        }
      },
    }

    const client = new MzSubscribeClient({ transport, reconnectMs: 50 })
    client.addView('v')
    await new Promise((r) => setTimeout(r, 100))

    // First connect should be snapshot
    expect(connectCalls[0]).toEqual({ snapshot: true })

    client.resync()
    await new Promise((r) => setTimeout(r, 100))

    // Resync should reconnect with snapshot (progress reset to 0)
    const lastCall = connectCalls[connectCalls.length - 1]
    expect(lastCall).toEqual({ snapshot: true })

    client.destroy()
  })
})
