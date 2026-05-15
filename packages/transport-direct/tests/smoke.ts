#!/usr/bin/env npx tsx
/**
 * Smoke test — run manually against a live Materialize instance.
 *
 * Usage:
 *   cd packages/transport-direct
 *   npx tsx tests/smoke.ts
 *
 * Env vars:
 *   MZ_HOST     (default: localhost:6878)
 *   MZ_USER     (default: materialize)
 *   MZ_PASSWORD (default: empty)
 *   MZ_VIEW_SQL (default: SELECT * FROM store_dashboard)
 *   MZ_KEY_COL  (default: id)
 */

import { MzDirectTransport } from '../src/direct-transport.js'
import { MzSubscribeClient } from '@mz-web/subscribe'
import type { MzBatch } from '@mz-web/subscribe'

const host = process.env.MZ_HOST ?? 'localhost:6878'
const user = process.env.MZ_USER ?? 'materialize'
const password = process.env.MZ_PASSWORD ?? ''
const viewSql = process.env.MZ_VIEW_SQL ?? 'SELECT * FROM store_dashboard'
const keyCol = process.env.MZ_KEY_COL ?? 'id'

console.log(`Connecting to ws://${host}/api/experimental/sql`)
console.log(`SUBSCRIBE (${viewSql}) KEY ("${keyCol}")`)
console.log()

const transport = new MzDirectTransport({
  host,
  auth: { user, password },
  tls: false,
})
transport.registerView('test', { sql: viewSql, keyColumn: keyCol })

const client = new MzSubscribeClient({ transport })

let batchCount = 0
client.addSink({
  onBatch(batch: MzBatch) {
    batchCount++
    const verb = batch.kind === 'snapshot' ? 'SNAPSHOT' : 'UPDATE'
    console.log(
      `[${verb}] batch #${batchCount}: ${batch.upserts.size} upserts, ${batch.deletes.size} deletes, progress=${batch.progress}`
    )
    if (batch.kind === 'snapshot') {
      console.log(`  ${batch.upserts.size} rows in initial snapshot`)
      const sample = batch.upserts.values().next().value
      if (sample) console.log('  sample:', JSON.stringify(sample))
    }
  },
  onConnectionChange(view, connected) {
    console.log(`[${connected ? 'CONNECTED' : 'DISCONNECTED'}] ${view}`)
  },
})

client.addView('test')
console.log('Waiting for data... (Ctrl-C to stop)\n')

// Keep alive
process.on('SIGINT', () => {
  console.log('\nShutting down...')
  client.destroy()
  process.exit(0)
})
