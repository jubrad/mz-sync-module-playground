/**
 * Minimal Web Worker for wa-sqlite.
 *
 * This worker is a dumb SQL executor — it knows nothing about tables,
 * schemas, or views. It receives SQL strings and executes them.
 *
 * Messages:
 *   IN:  { type: 'init', id }
 *   IN:  { type: 'exec', id, sql }
 *   IN:  { type: 'query', id, sql, params? }
 *   OUT: { type: 'ready' }
 *   OUT: { type: 'done', id }
 *   OUT: { type: 'result', id, rows }
 *   OUT: { type: 'error', id, error }
 */

import SQLiteESMFactory from 'wa-sqlite/dist/wa-sqlite.mjs'
// @ts-ignore
import * as SQLite from 'wa-sqlite/src/sqlite-api.js'

let sqlite: any = null
let db: number = 0

async function init(id: number) {
  try {
    const module = await SQLiteESMFactory()
    sqlite = SQLite.Factory(module)
    db = await sqlite.open_v2(':memory:')
    await sqlite.exec(db, 'PRAGMA journal_mode=WAL')
    await sqlite.exec(db, 'PRAGMA synchronous=OFF')
    self.postMessage({ type: 'ready', id })
  } catch (e) {
    self.postMessage({ type: 'error', id, error: String(e) })
  }
}

async function exec(id: number, sql: string) {
  try {
    await sqlite.exec(db, sql)
    self.postMessage({ type: 'done', id })
  } catch (e) {
    self.postMessage({ type: 'error', id, error: String(e) })
  }
}

async function query(id: number, sql: string, params?: unknown[]) {
  const rows: Record<string, unknown>[] = []
  try {
    const cb = (row: unknown[], columns: string[]) => {
      const obj: Record<string, unknown> = {}
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
    if (!msg.includes('no such table')) {
      console.error('[sqlite-wasm worker]', sql, e)
    }
    self.postMessage({ type: 'error', id, error: msg })
  }
}

self.onmessage = async (e: MessageEvent) => {
  const msg = e.data
  switch (msg.type) {
    case 'init':  return init(msg.id)
    case 'exec':  return exec(msg.id, msg.sql)
    case 'query': return query(msg.id, msg.sql, msg.params)
  }
}
