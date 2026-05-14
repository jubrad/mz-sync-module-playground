/**
 * MzSQLite Web Worker — owns the wa-sqlite database.
 *
 * All SQLite operations run here, off the main thread.
 * Uses prepared statements for high-throughput batch writes.
 */

import SQLiteESMFactory from 'wa-sqlite/dist/wa-sqlite.mjs'
// @ts-ignore
import * as SQLite from 'wa-sqlite/src/sqlite-api.js'

let sqlite: any = null
let db: number = 0
let version = 0

function escapeVal(v: unknown): string {
  if (v === null || v === undefined) return 'NULL'
  if (typeof v === 'number') return String(v)
  if (typeof v === 'boolean') return v ? '1' : '0'
  return `'${String(v).replace(/'/g, "''")}'`
}


async function init() {
  const module = await SQLiteESMFactory()
  sqlite = SQLite.Factory(module)
  db = await sqlite.open_v2(':memory:')
  await sqlite.exec(db, 'PRAGMA journal_mode=WAL')
  await sqlite.exec(db, 'PRAGMA synchronous=OFF')  // in-memory, no fsync needed
  self.postMessage({ type: 'ready' })
}

async function createTable(id: number, viewName: string, cols: { name: string; type: string }[], pk: string) {
  try {
    const colDefs = cols.map((c: any) => `"${c.name}" ${c.type.toUpperCase()}`).join(', ')
    await sqlite.exec(db, `CREATE TABLE IF NOT EXISTS "${viewName}" (${colDefs}, PRIMARY KEY ("${pk}"))`)

    // Cache metadata for commit()
    const colNames = cols.map((c: any) => c.name)
    tableMeta.set(viewName, { cols: colNames, pk })

    self.postMessage({ type: 'result', id })
  } catch (e) {
    self.postMessage({ type: 'result', id, error: String(e) })
  }
}


// Table metadata cache (set by createTable)
const tableMeta = new Map<string, { cols: string[]; pk: string }>()

async function commit(id: number, viewName: string, ops: any[]) {
  if (ops.length === 0) {
    self.postMessage({ type: 'committed', id, viewName, version })
    return
  }

  const meta = tableMeta.get(viewName)
  if (!meta) {
    self.postMessage({ type: 'committed', id, viewName, version, error: 'table not created yet: ' + viewName })
    return
  }

  try {
    const stmts: string[] = ['BEGIN']
    for (const op of ops) {
      if (op.type === 'upsert') {
        const vals = meta.cols.map((col: string) => {
          const v = col === meta.pk ? op.key : op.value?.[col]
          return escapeVal(v)
        })
        stmts.push(`INSERT OR REPLACE INTO "${viewName}" (${meta.cols.map((c: string) => `"${c}"`).join(',')}) VALUES (${vals.join(',')})`)
      } else {
        stmts.push(`DELETE FROM "${viewName}" WHERE "${meta.pk}" = ${escapeVal(op.key)}`)
      }
    }
    stmts.push('COMMIT')
    await sqlite.exec(db, stmts.join(';\n'))
    version++
    self.postMessage({ type: 'committed', id, viewName, version })
  } catch (e) {
    try { await sqlite.exec(db, 'ROLLBACK') } catch {}
    console.error('[worker] commit error:', viewName, ops.length, 'ops', e)
    self.postMessage({ type: 'committed', id, viewName, version, error: String(e) })
  }
}

/** Batch commit — multiple views in a single BEGIN/COMMIT. */
async function commitBatch(id: number, batch: { viewName: string; ops: any[] }[]) {
  try {
    const stmts: string[] = ['BEGIN']
    for (const { viewName, ops } of batch) {
      const meta = tableMeta.get(viewName)
      if (!meta || ops.length === 0) continue
      for (const op of ops) {
        if (op.type === 'upsert') {
          const vals = meta.cols.map((col: string) => {
            const v = col === meta.pk ? op.key : op.value?.[col]
            return escapeVal(v)
          })
          stmts.push(`INSERT OR REPLACE INTO "${viewName}" (${meta.cols.map((c: string) => `"${c}"`).join(',')}) VALUES (${vals.join(',')})`)
        } else {
          stmts.push(`DELETE FROM "${viewName}" WHERE "${meta.pk}" = ${escapeVal(op.key)}`)
        }
      }
    }
    stmts.push('COMMIT')
    await sqlite.exec(db, stmts.join(';\n'))
    version++
    // Notify for all views in the batch
    const viewNames = batch.map((b) => b.viewName)
    self.postMessage({ type: 'batchCommitted', id, viewNames, version })
  } catch (e) {
    try { await sqlite.exec(db, 'ROLLBACK') } catch {}
    console.error('[worker] batch commit error:', e)
    self.postMessage({ type: 'batchCommitted', id, viewNames: [], version, error: String(e) })
  }
}

async function query(id: number, sql: string, params: any[]) {
  const rows: any[] = []
  try {
    const cb = (row: any, columns: any) => {
      const obj: any = {}
      for (let i = 0; i < columns.length; i++) {
        obj[columns[i]] = row[i]
      }
      rows.push(obj)
    }
    if (params && params.length) {
      await sqlite.execWithParams(db, sql, params, cb)
    } else {
      await sqlite.exec(db, sql, cb)
    }
    self.postMessage({ type: 'result', id, rows })
  } catch (e) {
    const msg = String(e)
    if (!msg.includes('no such table') && !msg.includes('memory access')) {
      console.error('[worker] query error:', sql, e)
    }
    self.postMessage({ type: 'result', id, rows: [], error: msg })
  }
}

// ── Message handler ────────────────────────────────────────────

self.onmessage = async (e: MessageEvent) => {
  const msg = e.data
  switch (msg.type) {
    case 'init':
      await init()
      break
    case 'createTable':
      await createTable(msg.id, msg.viewName, msg.cols, msg.pk)
      break
    case 'commit':
      await commit(msg.id, msg.viewName, msg.ops)
      break
    case 'query':
      await query(msg.id, msg.sql, msg.params)
      break
  }
}
