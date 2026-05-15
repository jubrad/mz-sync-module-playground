/**
 * Integration test for @mz-web/transport-direct.
 *
 * Self-contained: creates its own MZ table + view, tests, cleans up.
 * Only requires a running Materialize instance — no demo schema needed.
 *
 * Skip with: SKIP_INTEGRATION=1 npx vitest run
 *
 * Start MZ:
 *   docker run -d -p 6878:6875 materialize/materialized:latest
 *
 * Run:
 *   cd packages/transport-direct
 *   npx vitest run tests/integration.test.ts
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { MzDirectTransport } from '../src/direct-transport.js'
import { MzSubscribeClient } from '@mz-web/subscribe'
import type { MzBatch } from '@mz-web/subscribe'
import WebSocket from 'ws'

const SKIP = process.env.SKIP_INTEGRATION === '1'
// HTTP/WS API port (6876 inside container)
const MZ_HOST = process.env.MZ_HOST ?? 'localhost:6876'
// SQL port for setup/teardown via psql-over-ws (same WS API)
const MZ_SQL_HOST = process.env.MZ_SQL_HOST ?? MZ_HOST
const MZ_USER = process.env.MZ_USER ?? 'materialize'
const MZ_PASSWORD = process.env.MZ_PASSWORD ?? ''

// Make ws available globally for the transport (it expects browser WebSocket)
;(globalThis as any).WebSocket = WebSocket

/** Run SQL against MZ via its WebSocket API (one-shot, for setup/teardown). */
function mzExec(sql: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(`ws://${MZ_SQL_HOST}/api/experimental/sql`)
    ws.on('open', () => {
      ws.send(JSON.stringify({ user: MZ_USER, password: MZ_PASSWORD }))
    })
    let authed = false
    ws.on('message', (data: Buffer) => {
      const msg = JSON.parse(data.toString())
      if (msg.type === 'ReadyForQuery' && !authed) {
        authed = true
        ws.send(JSON.stringify({ query: sql }))
      } else if (msg.type === 'ReadyForQuery' && authed) {
        ws.close()
        resolve()
      } else if (msg.type === 'Error') {
        ws.close()
        if (msg.payload?.message?.includes('already exists') ||
            msg.payload?.message?.includes('does not exist') ||
            msg.payload?.message?.includes('unknown catalog item')) {
          resolve()
        } else {
          reject(new Error(msg.payload?.message ?? 'MZ error'))
        }
      }
    })
    ws.on('error', (err: Error) => reject(err))
    setTimeout(() => { ws.close(); reject(new Error('mzExec timeout')) }, 10_000)
  })
}

const TEST_TABLE = '_mz_transport_test'
const TEST_VIEW = '_mz_transport_test_view'

describe.skipIf(SKIP)('MzDirectTransport integration', () => {
  beforeAll(async () => {
    // Clean slate
    try { await mzExec(`DROP MATERIALIZED VIEW IF EXISTS ${TEST_VIEW}`) } catch {}
    try { await mzExec(`DROP TABLE IF EXISTS ${TEST_TABLE}`) } catch {}

    // Create test objects
    await mzExec(`CREATE TABLE ${TEST_TABLE} (id INT, name TEXT, score DOUBLE PRECISION)`)
    await mzExec(`CREATE MATERIALIZED VIEW ${TEST_VIEW} AS SELECT * FROM ${TEST_TABLE}`)
    await mzExec(`CREATE DEFAULT INDEX ON ${TEST_VIEW}`)
    await mzExec(`INSERT INTO ${TEST_TABLE} VALUES (1, 'alice', 95.5), (2, 'bob', 87.3), (3, 'carol', 92.1)`)
  }, 30_000)

  afterAll(async () => {
    try { await mzExec(`DROP MATERIALIZED VIEW IF EXISTS ${TEST_VIEW}`) } catch {}
    try { await mzExec(`DROP TABLE IF EXISTS ${TEST_TABLE}`) } catch {}
  }, 15_000)

  function makeClient() {
    const transport = new MzDirectTransport({
      host: MZ_HOST,
      auth: { user: MZ_USER, password: MZ_PASSWORD },
      tls: false,
    })
    transport.registerView('test', {
      sql: `SELECT * FROM ${TEST_VIEW}`,
      keyColumn: 'id',
    })
    return new MzSubscribeClient({ transport })
  }

  it('connects and receives a snapshot with 3 rows', async () => {
    const batches: MzBatch[] = []
    const client = makeClient()
    client.addSink({ onBatch(b) { batches.push(b) } })
    client.addView('test')

    const deadline = Date.now() + 10_000
    while (batches.length === 0 && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 100))
    }

    expect(batches.length).toBeGreaterThan(0)
    const snap = batches[0]
    expect(snap.view).toBe('test')
    expect(snap.upserts.size).toBe(3)

    const alice = snap.upserts.get('1')
    expect(alice).toBeDefined()
    expect(alice!.name).toBe('alice')
    expect(Number(alice!.score)).toBeCloseTo(95.5)

    console.log(`  snapshot: ${snap.upserts.size} rows, progress=${snap.progress}`)
    client.destroy()
  }, 15_000)

  it('receives live insert', async () => {
    const batches: MzBatch[] = []
    const client = makeClient()
    client.addSink({ onBatch(b) { batches.push(b) } })
    client.addView('test')

    // Wait for snapshot
    while (!batches.some((b) => b.kind === 'snapshot')) {
      await new Promise((r) => setTimeout(r, 100))
    }
    const before = batches.length

    await mzExec(`INSERT INTO ${TEST_TABLE} VALUES (4, 'dave', 88.0)`)

    const deadline = Date.now() + 10_000
    while (!batches.some((b) => b.upserts.has('4')) && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 100))
    }

    const update = batches.find((b) => b.upserts.has('4'))
    expect(update).toBeDefined()
    expect(update!.upserts.get('4')!.name).toBe('dave')

    console.log(`  insert received in batch #${batches.indexOf(update!)}`)
    client.destroy()
  }, 20_000)

  it('receives live delete', async () => {
    const batches: MzBatch[] = []
    const client = makeClient()
    client.addSink({ onBatch(b) { batches.push(b) } })
    client.addView('test')

    // Wait for snapshot
    while (!batches.some((b) => b.kind === 'snapshot')) {
      await new Promise((r) => setTimeout(r, 100))
    }

    await mzExec(`DELETE FROM ${TEST_TABLE} WHERE id = 4`)

    const deadline = Date.now() + 10_000
    while (!batches.some((b) => b.deletes.has('4')) && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 100))
    }

    const del = batches.find((b) => b.deletes.has('4'))
    expect(del).toBeDefined()

    console.log(`  delete received in batch #${batches.indexOf(del!)}`)
    client.destroy()
  }, 20_000)

  it('tracks progress and marks ready', async () => {
    const batches: MzBatch[] = []
    const client = makeClient()
    client.addSink({ onBatch(b) { batches.push(b) } })
    client.addView('test')

    // Wait for at least one batch (snapshot)
    const deadline = Date.now() + 10_000
    while (batches.length === 0 && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 100))
    }

    // Give the client a moment to update its view handle
    await new Promise((r) => setTimeout(r, 200))

    const h = client.getView('test')
    expect(h).toBeDefined()
    expect(h!.progress).toBeGreaterThan(0)
    expect(h!.ready).toBe(true)
    expect(h!.connected).toBe(true)

    console.log(`  progress=${h!.progress}, ready=${h!.ready}`)
    client.destroy()
  }, 15_000)
})
