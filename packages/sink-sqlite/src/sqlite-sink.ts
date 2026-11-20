import type { MzBatch, MzSink } from '@mz-web/subscribe'
import type { SqliteExec, TableSchema, SchemaMode, SinkSqliteOpts } from './types.js'
import { inferSchema, resolveSchema } from './schema.js'
import { createTableDDL, batchWriteSQL, escapeIdent } from './table-writer.js'

interface TableState {
  schema: TableSchema | null
  created: boolean
}

/**
 * SQLite sink — implements MzSink from @mz-web/subscribe.
 *
 * Receives batches of changes and writes them to a SQLite database
 * via the SqliteExec interface. Tables are auto-created from server
 * schema, explicit definitions, or inferred from the first row.
 */
export class MzSqliteSink implements MzSink {
  private db: SqliteExec
  private schemaMode: SchemaMode | undefined
  private serverBaseUrl: string | undefined
  private onCommitCb: ((view: string, version: number) => void) | undefined
  private tables = new Map<string, TableState>()
  private listeners = new Set<() => void>()
  private tableListeners = new Map<string, Set<() => void>>()

  private _version = 0
  private _lastCommitTime = 0
  private _totalWrites = 0
  private _totalCommits = 0
  private _totalQueries = 0

  constructor(opts: SinkSqliteOpts) {
    this.db = opts.db
    this.schemaMode = opts.schema
    this.serverBaseUrl = opts.serverBaseUrl
    this.onCommitCb = opts.onCommit
  }

  // ── MzSink interface ───────────────────────────────

  async onBatch(batch: MzBatch): Promise<void> {
    const { view, upserts, deletes } = batch
    if (upserts.size === 0 && deletes.size === 0) return

    // Ensure table exists
    let table = this.tables.get(view)
    if (!table) {
      table = { schema: null, created: false }
      this.tables.set(view, table)
    }

    if (!table.created) {
      // Try to resolve schema
      if (!table.schema) {
        table.schema = await resolveSchema(view, this.schemaMode, this.serverBaseUrl)
      }
      // If still no schema, infer from first upsert
      if (!table.schema && upserts.size > 0) {
        const firstRow = upserts.values().next().value!
        // Use 'id' if the row has it; otherwise fall back to '_key' as a
        // synthetic PK column name. Using a fixed column name avoids the
        // previous brittle heuristic that used the actual key *value* as
        // the column name.
        const pk = 'id' in firstRow ? 'id' : '_key'
        table.schema = inferSchema(firstRow, pk)
      }
      if (!table.schema) {
        // Delete-only batch with no schema available — we cannot process
        // deletes without knowing the table structure.
        if (deletes.size > 0) {
          console.warn(`[sink-sqlite] dropping ${deletes.size} delete(s) for "${view}": no schema available yet`)
        }
        return
      }

      await this.db.exec(createTableDDL(view, table.schema))
      table.created = true
    }

    // Write batch
    const sql = batchWriteSQL(view, table.schema!, upserts, deletes)
    try {
      await this.db.exec(sql)
    } catch (e) {
      console.error(`[sink-sqlite] write error for ${view}:`, e)
      // Attempt rollback
      try { await this.db.exec('ROLLBACK') } catch (re) { console.error('[sink-sqlite] ROLLBACK failed:', re) }
      return
    }

    this._totalWrites += upserts.size + deletes.size
    this._totalCommits++
    this._version++
    this._lastCommitTime = Date.now()

    this.onCommitCb?.(view, this._version)
    this.notify(view)
  }

  // ── Query interface ────────────────────────────────

  /** Run a SQL query against the database. */
  async query<T = Record<string, unknown>>(sql: string, params?: unknown[]): Promise<T[]> {
    this._totalQueries++
    return this.db.query<T>(sql, params)
  }

  // ── Subscriptions (for React hooks) ────────────────

  /** Subscribe to commits on a specific table. Returns unsubscribe. */
  subscribeTable(view: string, cb: () => void): () => void {
    let set = this.tableListeners.get(view)
    if (!set) { set = new Set(); this.tableListeners.set(view, set) }
    set.add(cb)
    return () => { set!.delete(cb) }
  }

  /** Subscribe to any commit. Returns unsubscribe. */
  subscribe(cb: () => void): () => void {
    this.listeners.add(cb)
    return () => this.listeners.delete(cb)
  }

  // ── Diagnostics ────────────────────────────────────

  get version(): number { return this._version }
  get lastCommitTime(): number { return this._lastCommitTime }
  get totalWrites(): number { return this._totalWrites }
  get totalCommits(): number { return this._totalCommits }
  get totalQueries(): number { return this._totalQueries }

  async getDiagnostics(): Promise<{
    tables: { name: string; rows: number }[]
    totalWrites: number
    totalCommits: number
    totalQueries: number
    version: number
    /** Size of the database in bytes (pages × page size), or 0 if unknown. */
    dbSizeBytes: number
  }> {
    const tables: { name: string; rows: number }[] = []
    for (const [name, state] of this.tables) {
      if (!state.created) continue
      try {
        const r = await this.db.query<{ cnt: number }>(`SELECT count(*) as cnt FROM ${escapeIdent(name)}`)
        tables.push({ name, rows: r[0]?.cnt ?? 0 })
      } catch {
        tables.push({ name, rows: 0 })
      }
    }
    let dbSizeBytes = 0
    try {
      const pages = await this.db.query<{ page_count: number }>('PRAGMA page_count')
      const size = await this.db.query<{ page_size: number }>('PRAGMA page_size')
      dbSizeBytes = (pages[0]?.page_count ?? 0) * (size[0]?.page_size ?? 0)
    } catch {
      // Not every SqliteExec exposes pragmas as queries.
    }
    return {
      tables,
      dbSizeBytes,
      totalWrites: this._totalWrites,
      totalCommits: this._totalCommits,
      totalQueries: this._totalQueries,
      version: this._version,
    }
  }

  // ── Internal ───────────────────────────────────────

  private notify(view: string): void {
    const tableSet = this.tableListeners.get(view)
    const notified = new Set<() => void>()
    if (tableSet) {
      for (const cb of tableSet) { notified.add(cb); try { cb() } catch {} }
    }
    for (const cb of this.listeners) {
      if (!notified.has(cb)) { try { cb() } catch {} }
    }
  }
}
