import { describe, it, expect } from 'vitest'
import { WasmSqlite } from '../src/wasm-sqlite.js'

describe('WasmSqlite', () => {
  it('can be constructed', () => {
    const db = new WasmSqlite({ workerUrl: '/worker.js' })
    expect(db).toBeDefined()
  })

  it('throws without workerUrl', async () => {
    const db = new WasmSqlite()
    await expect(db.init()).rejects.toThrow('workerUrl is required')
  })

  it('throws on exec before init', async () => {
    const db = new WasmSqlite({ workerUrl: '/worker.js' })
    await expect(db.exec('SELECT 1')).rejects.toThrow('not initialized')
  })

  it('returns empty array on query before init', async () => {
    const db = new WasmSqlite({ workerUrl: '/worker.js' })
    const rows = await db.query('SELECT 1')
    expect(rows).toEqual([])
  })

  // Integration tests with actual wa-sqlite WASM require a browser environment
  // or a bundler that can load the WASM module. Those would go in e2e tests.
})
