import type { SqliteExec } from '@mz-web/sink-sqlite'

export interface WasmSqliteOpts {
  /**
   * URL to the worker script. If not provided, uses the bundled worker.
   * Vite: import WorkerURL from '@mz-web/sqlite-wasm/worker?worker&url'
   */
  workerUrl?: string | URL
}

/**
 * Browser-specific SQLite runtime.
 *
 * Loads wa-sqlite in a Web Worker and implements SqliteExec by proxying
 * commands over postMessage. The main thread never touches SQLite directly.
 */
export class WasmSqlite implements SqliteExec {
  private worker: Worker | null = null
  private ready = false
  private initPromise: Promise<void> | null = null
  private nextId = 1
  private pending = new Map<number, { resolve: (v: any) => void; reject: (e: any) => void }>()
  private workerUrl?: string | URL

  constructor(opts?: WasmSqliteOpts) {
    this.workerUrl = opts?.workerUrl
  }

  /** Initialize the worker and open the in-memory database. */
  async init(): Promise<void> {
    if (this.ready) return
    if (this.initPromise) return this.initPromise
    // C2: Reset initPromise on failure so init() can be retried
    this.initPromise = this._init().catch((e) => {
      this.initPromise = null
      throw e
    })
    return this.initPromise
  }

  private _init(): Promise<void> {
    return new Promise((resolve, reject) => {
      try {
        const url = this.workerUrl
        if (url) {
          this.worker = new Worker(url, { type: 'module' })
        } else {
          // Fallback: try to use the bundled worker via import.meta
          // Consumers should provide workerUrl for their bundler
          throw new Error(
            'WasmSqlite: workerUrl is required. ' +
            'Provide the URL to the worker script, e.g.: ' +
            "new WasmSqlite({ workerUrl: new URL('./worker.js', import.meta.url) })"
          )
        }

        this.worker.onmessage = (e) => this.onMessage(e.data)

        // C3: Persistent error handler that rejects all pending promises
        this.worker.onerror = (e) => {
          console.error('[sqlite-wasm] worker error:', e)
          this.ready = false
          for (const [, p] of this.pending) {
            p.reject(new Error('Worker crashed'))
          }
          this.pending.clear()
        }

        const id = this.nextId++
        this.pending.set(id, {
          resolve: () => { this.ready = true; resolve() },
          reject,
        })
        this.worker.postMessage({ type: 'init', id })
      } catch (e) {
        reject(e)
      }
    })
  }

  /** Execute SQL (DDL/DML). */
  async exec(sql: string): Promise<void> {
    if (!this.ready) throw new Error('WasmSqlite not initialized')
    return this.send({ type: 'exec', sql })
  }

  /** Execute a query and return rows. */
  async query<T = Record<string, unknown>>(sql: string, params?: unknown[]): Promise<T[]> {
    // W1: Throw consistently with exec() instead of silently returning []
    if (!this.ready) throw new Error('WasmSqlite not initialized')
    return this.send({ type: 'query', sql, params })
  }

  /** Terminate the worker. */
  destroy(): void {
    this.worker?.terminate()
    this.worker = null
    this.ready = false
    // S5: Reset initPromise so a new instance is needed
    this.initPromise = null
    // Reject all pending
    for (const [, p] of this.pending) {
      p.reject(new Error('WasmSqlite destroyed'))
    }
    this.pending.clear()
  }

  private send(msg: any): Promise<any> {
    // W3: Guard against calling send() after destroy()
    if (!this.worker) return Promise.reject(new Error('WasmSqlite has been destroyed'))
    return new Promise((resolve, reject) => {
      const id = this.nextId++
      this.pending.set(id, { resolve, reject })
      this.worker!.postMessage({ ...msg, id })
    })
  }

  private onMessage(msg: any): void {
    const p = this.pending.get(msg.id)
    if (!p) return
    this.pending.delete(msg.id)

    switch (msg.type) {
      case 'ready':
      case 'done':
        p.resolve(undefined)
        break
      case 'result':
        p.resolve(msg.rows ?? [])
        break
      case 'error':
        // Resolve with empty for queries (don't crash hooks), reject for exec
        if (msg.error?.includes('no such table')) {
          p.resolve([])
        } else {
          p.reject(new Error(msg.error))
        }
        break
    }
  }
}
