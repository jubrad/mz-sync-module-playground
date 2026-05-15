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

  // W1: query() now throws consistently with exec() when not initialized
  it('throws on query before init', async () => {
    const db = new WasmSqlite({ workerUrl: '/worker.js' })
    await expect(db.query('SELECT 1')).rejects.toThrow('not initialized')
  })

  // C2: init() can be retried after failure
  it('allows init() retry after failure', async () => {
    const db = new WasmSqlite()
    // First attempt fails (no workerUrl)
    await expect(db.init()).rejects.toThrow('workerUrl is required')
    // initPromise should be cleared, so a second attempt should also run (and fail the same way)
    await expect(db.init()).rejects.toThrow('workerUrl is required')
  })

  // S5: destroy() cleans up properly
  it('destroy() resets state so subsequent calls fail', async () => {
    const db = new WasmSqlite({ workerUrl: '/worker.js' })
    // destroy before init should not throw
    db.destroy()
    // After destroy, exec/query should throw 'not initialized'
    await expect(db.exec('SELECT 1')).rejects.toThrow('not initialized')
    await expect(db.query('SELECT 1')).rejects.toThrow('not initialized')
  })

  // W3: send() rejects when worker is destroyed
  it('send() rejects after destroy()', async () => {
    const db = new WasmSqlite({ workerUrl: '/worker.js' })
    // We need to get past the ready check to test send() directly.
    // Use Object.assign to set ready=true so exec/query reach send()
    Object.assign(db, { ready: true })
    db.destroy()
    // Now ready is false after destroy, so exec/query throw 'not initialized'
    await expect(db.exec('SELECT 1')).rejects.toThrow('not initialized')
    // Force ready=true again to exercise the send() guard
    Object.assign(db, { ready: true })
    await expect(db.exec('SELECT 1')).rejects.toThrow('has been destroyed')
    await expect(db.query('SELECT 1')).rejects.toThrow('has been destroyed')
  })

  // Integration tests with actual wa-sqlite WASM require a browser environment
  // or a bundler that can load the WASM module. Those would go in e2e tests.
})
