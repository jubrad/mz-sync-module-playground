/**
 * MzSQLite — main-thread client for the SQLite Web Worker.
 *
 * The heavy lifting (wa-sqlite WASM, all reads/writes) runs in a Worker.
 * This class manages transports (WebSocket), buffers events, and forwards
 * them to the worker for DB operations. Queries go through postMessage.
 *
 * Public API is unchanged — hooks don't know about the worker.
 */
import { MzTransport } from './mz-sync'
import type { ChangeEvent, MzTimestamp, Transport } from './types'
// Vite handles this import as a Worker URL
import WorkerURL from './mz-sqlite-worker?worker&url'

// ── Types (same as before) ─────────────────────────────────────

type SqliteType = 'text' | 'integer' | 'real'

export interface ColumnDef {
  type?: SqliteType
  optional?: boolean
}

export interface ServerSchema {
  name: string
  key_column: string
  columns: { name: string; pg_type: string }[]
}

export interface SyncOptions {
  columns?: Record<string, SqliteType | ColumnDef>
  primaryKey?: string
  schema?: 'server' | 'infer' | 'explicit'
}

export interface MzSQLiteOptions {
  transport?: Transport
  reconnectMs?: number
}

// ── PG type mapping ────────────────────────────────────────────

function pgTypeToSqlite(pgType: string): SqliteType {
  switch (pgType) {
    case 'int2': case 'int4': case 'int8': case 'oid': case 'bool': return 'integer'
    case 'float4': case 'float8': case 'numeric': return 'real'
    default: return 'text'
  }
}

function jsTypeToSqlite(val: unknown): SqliteType {
  if (typeof val === 'number') return Number.isInteger(val) ? 'integer' : 'real'
  if (typeof val === 'boolean') return 'integer'
  return 'text'
}

// ── MzSQLite (main thread) ─────────────────────────────────────

export class MzSQLite {
  private baseUrl: string
  private transportType: Transport
  private reconnectMs: number

  private worker: Worker | null = null
  private ready = false
  private initPromise: Promise<void> | null = null

  private views = new Map<string, ViewCtx>()
  private flushId: number | null = null

  private listeners = new Set<() => void>()
  private tableListeners = new Map<string, Set<() => void>>()

  private version = 0
  private nextId = 1
  private pending = new Map<number, { resolve: (v: any) => void; reject: (e: any) => void }>()

  // Diagnostics
  private _stats = {
    totalWrites: 0,
    totalQueries: 0,
    totalCommits: 0,
    writesPerSec: 0,
    _writeWindow: [] as number[],
  }

  constructor(baseUrl: string, opts?: MzSQLiteOptions) {
    this.baseUrl = baseUrl.replace(/\/$/, '') || `${window.location.protocol}//${window.location.host}`
    this.transportType = opts?.transport || 'ws'
    this.reconnectMs = opts?.reconnectMs ?? 2000
  }

  async init() {
    if (this.ready) return
    if (this.initPromise) return this.initPromise
    this.initPromise = this._init()
    return this.initPromise
  }

  private _init(): Promise<void> {
    return new Promise((resolve, reject) => {
      try {
        this.worker = new Worker(WorkerURL, { type: 'module' })
        this.worker.onerror = (e) => {
          console.error('[mz-sqlite] worker error:', e)
          reject(e)
        }
        this.worker.onmessage = (e) => {
          const msg = e.data
          if (msg.type === 'ready') {
            this.ready = true
            resolve()
            return
          }
          this.onWorkerMessage(msg)
        }
        this.worker.postMessage({ type: 'init' })
      } catch (e) {
        reject(e)
      }
    })
  }

  // ── Worker communication ────────────────────────────

  private send(msg: any): Promise<any> {
    return new Promise((resolve, reject) => {
      const id = this.nextId++
      this.pending.set(id, { resolve, reject })
      this.worker!.postMessage({ ...msg, id })
    })
  }

  private onWorkerMessage(msg: any) {
    if (msg.type === 'ready') return

    if (msg.type === 'committed') {
      if (msg.error) console.warn('[mz-sqlite] commit error:', msg.viewName, msg.error)
      this.version = msg.version ?? this.version + 1
      this.lastCommitTime = Date.now()
      this._stats.totalCommits++
      const p = this.pending.get(msg.id)
      if (p) { this.pending.delete(msg.id); p.resolve(undefined) }
      this.notify(msg.viewName)
      return
    }

    if (msg.type === 'result') {
      const p = this.pending.get(msg.id)
      if (p) {
        this.pending.delete(msg.id)
        if (msg.error) { console.warn('[mz-sqlite] query error:', msg.error); p.resolve(msg.rows ?? []) }
        else p.resolve(msg.rows)
      }
      return
    }
  }

  // ── Public API (unchanged) ──────────────────────────

  async sync(viewName: string, opts?: SyncOptions) {
    if (!this.ready) await this.init()

    let ctx = this.views.get(viewName)
    if (ctx) { ctx.refCount++; return }

    const mode = opts?.columns ? 'explicit' : (opts?.schema ?? 'server')
    let resolvedCols: { name: string; type: SqliteType }[] = []
    let primaryKey = opts?.primaryKey ?? ''

    if (mode === 'explicit') {
      resolvedCols = Object.entries(opts!.columns!).map(([name, def]) => ({
        name, type: typeof def === 'string' ? def : (def.type ?? 'text'),
      }))
      if (!primaryKey) primaryKey = resolvedCols[0]?.name ?? 'id'
    } else if (mode === 'server') {
      const server = await this.fetchSchema(viewName)
      if (server) {
        resolvedCols = server.columns.map((c) => ({ name: c.name, type: pgTypeToSqlite(c.pg_type) }))
        primaryKey = opts?.primaryKey ?? server.key_column
      }
    }

    const needsInfer = mode === 'infer' || (mode === 'server' && resolvedCols.length === 0)

    if (!needsInfer) {
      console.log(`[mz-sqlite] createTable ${viewName}: ${resolvedCols.map(c => c.name).join(', ')} (pk=${primaryKey})`)
      const result = await this.send({ type: 'createTable', viewName, cols: resolvedCols, pk: primaryKey })
      if (result?.error) console.error(`[mz-sqlite] createTable failed:`, result.error)
    }

    ctx = {
      resolvedCols: needsInfer ? null : resolvedCols,
      primaryKey,
      refCount: 1,
      progress: 0,
      transport: null,
      pendingOps: [],
      tableCreated: !needsInfer,
    }

    this.views.set(viewName, ctx)
    this.tableListeners.set(viewName, new Set())

    const transport = new MzTransport(this.baseUrl, viewName, {
      onEvent: (ev) => this.handleEvent(viewName, ev),
      onConnected: () => this.notify(viewName),
      onDisconnected: () => this.notify(viewName),
      getProgress: () => this.views.get(viewName)?.progress ?? 0,
    }, { transport: this.transportType, reconnectMs: this.reconnectMs })

    ctx.transport = transport
    transport.connect()
  }

  async openView(viewName: string, opts?: SyncOptions) {
    return this.sync(viewName, opts)
  }

  closeView(viewName: string) {
    const ctx = this.views.get(viewName)
    if (!ctx) return
    ctx.refCount--
    if (ctx.refCount <= 0) {
      ctx.transport?.disconnect()
      this.views.delete(viewName)
      this.tableListeners.delete(viewName)
    }
  }

  /** Query SQLite via the worker. Returns rows as objects. */
  async queryAsync<T = Record<string, unknown>>(sql: string, params: any[] = []): Promise<T[]> {
    if (!this.ready) return []
    this._stats.totalQueries++
    return this.send({ type: 'query', sql, params })
  }

  getVersion() { return this.version }

  /** Wall-clock time (Date.now()) of the last committed batch. */
  getLastCommitTime(): number { return this.lastCommitTime }
  private lastCommitTime = 0

  /** Get diagnostics snapshot. */
  async getDiagnostics(): Promise<DiagnosticsInfo> {
    const tables: { name: string; rows: number; connected: boolean }[] = []
    for (const [name, ctx] of this.views) {
      let rows = 0
      try {
        const r = await this.queryAsync<{ cnt: number }>(`SELECT count(*) as cnt FROM "${name}"`)
        rows = r[0]?.cnt ?? 0
      } catch { /* table may not exist yet */ }
      tables.push({ name, rows, connected: ctx.transport?.connected ?? false })
    }
    let dbSizeBytes = 0
    try {
      const r = await this.queryAsync<{ page_count: number; page_size: number }>(
        'SELECT page_count, page_size FROM pragma_page_count(), pragma_page_size()'
      )
      if (r[0]) dbSizeBytes = (r[0].page_count ?? 0) * (r[0].page_size ?? 0)
    } catch {
      // Fallback: try separate pragmas
      try {
        const pc = await this.queryAsync<any>('PRAGMA page_count')
        const ps = await this.queryAsync<any>('PRAGMA page_size')
        dbSizeBytes = (pc[0]?.page_count ?? 0) * (ps[0]?.page_size ?? 0)
      } catch {}
    }
    return {
      tables,
      totalWrites: this._stats.totalWrites,
      totalCommits: this._stats.totalCommits,
      totalQueries: this._stats.totalQueries,
      version: this.version,
      dbSizeBytes,
    }
  }

  subscribeTable(viewName: string, cb: () => void): () => void {
    let set = this.tableListeners.get(viewName)
    if (!set) { set = new Set(); this.tableListeners.set(viewName, set) }
    set.add(cb)
    this.listeners.add(cb)
    return () => { set!.delete(cb); this.listeners.delete(cb) }
  }

  subscribe(cb: () => void): () => void {
    this.listeners.add(cb)
    return () => this.listeners.delete(cb)
  }

  /** Clear all local data and reconnect all views with fresh snapshots. */
  async resync() {
    for (const [viewName, ctx] of this.views) {
      ctx.transport?.disconnect()
      ctx.pendingOps = []
      ctx.progress = 0
      // Drop and recreate table
      if (ctx.tableCreated) {
        await this.send({ type: 'query', sql: `DELETE FROM "${viewName}"` })
      }
      // Reconnect
      ctx.transport?.connect()
    }
    this.version++
    this.notify('')
  }

  destroy() {
    if (this.flushId !== null) cancelAnimationFrame(this.flushId)
    for (const [, ctx] of this.views) ctx.transport?.disconnect()
    this.worker?.terminate()
    this.views.clear()
  }

  // ── Event handling (main thread) ────────────────────

  private handleEvent(viewName: string, event: ChangeEvent) {
    const ctx = this.views.get(viewName)
    if (!ctx) return

    switch (event.type) {
      case 'upsert': {
        let val: any
        try {
          val = typeof event.value === 'string' ? JSON.parse(event.value) : event.value
        } catch { val = {} }
        ctx.pendingOps.push({ type: 'upsert', key: event.key!, value: val })
        this.scheduleFlush()
        break
      }
      case 'delete': {
        ctx.pendingOps.push({ type: 'delete', key: event.key! })
        this.scheduleFlush()
        break
      }
      case 'progress': {
        ctx.progress = Math.max(ctx.progress, event.timestamp ?? 0)
        this.flushView(viewName)
        break
      }
      case 'snapshot_complete':
      case 'catchup_complete': {
        ctx.progress = Math.max(ctx.progress, event.timestamp ?? ctx.progress)
        this.flushView(viewName)
        break
      }
    }
  }

  private async flushView(viewName: string) {
    const ctx = this.views.get(viewName)
    if (!ctx || ctx.pendingOps.length === 0) return

    // Infer schema from first row if needed
    if (!ctx.tableCreated) {
      const firstUpsert = ctx.pendingOps.find((op) => op.type === 'upsert')
      if (firstUpsert && firstUpsert.type === 'upsert' && firstUpsert.value) {
        const pk = ctx.primaryKey || 'id'
        const cols: { name: string; type: SqliteType }[] = [{ name: pk, type: 'text' }]
        for (const [k, v] of Object.entries(firstUpsert.value)) {
          if (k === pk) { cols[0].type = jsTypeToSqlite(v); continue }
          cols.push({ name: k, type: jsTypeToSqlite(v) })
        }
        await this.send({ type: 'createTable', viewName, cols, pk })
        ctx.resolvedCols = cols
        ctx.primaryKey = pk
        ctx.tableCreated = true
      } else {
        return
      }
    }

    const ops = ctx.pendingOps.splice(0)
    this._stats.totalWrites += ops.length
    this._stats._writeWindow.push(ops.length)
    this.send({ type: 'commit', viewName, ops })
  }

  private scheduleFlush() {
    if (this.flushId !== null) return
    this.flushId = requestAnimationFrame(() => {
      this.flushId = null
      for (const [name, ctx] of this.views) {
        if (ctx.pendingOps.length > 0) {
          this.flushView(name)
        }
      }
    })
  }

  private notify(viewName: string) {
    const set = this.tableListeners.get(viewName)
    if (set) for (const cb of set) { try { cb() } catch {} }
    for (const cb of this.listeners) { try { cb() } catch {} }
  }

  private async fetchSchema(viewName: string): Promise<ServerSchema | null> {
    try {
      const r = await fetch(`${this.baseUrl}/api/views/${encodeURIComponent(viewName)}/schema`)
      if (!r.ok) return null
      return await r.json()
    } catch { return null }
  }
}

// ── Internal types ─────────────────────────────────────────────

interface PendingOp {
  type: 'upsert' | 'delete'
  key: string
  value?: Record<string, unknown>
}

export interface DiagnosticsInfo {
  tables: { name: string; rows: number; connected: boolean }[]
  totalWrites: number
  totalCommits: number
  totalQueries: number
  version: number
  dbSizeBytes: number
}

interface ViewCtx {
  resolvedCols: { name: string; type: SqliteType }[] | null
  primaryKey: string
  refCount: number
  progress: MzTimestamp
  transport: MzTransport | null
  pendingOps: PendingOp[]
  tableCreated: boolean
}
