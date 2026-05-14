/**
 * React hooks for MzSQLite — run live SQL queries against local wa-sqlite.
 *
 * Since wa-sqlite is async, we cache the last query result and return it
 * synchronously via useSyncExternalStore. After each commit notification,
 * we re-run the query async and update the cache.
 */
import { createContext, useContext, useEffect, useMemo, useRef, useState, useCallback } from 'react'
import type { MzSQLite } from './mz-sqlite'

// ── Context ────────────────────────────────────────────────────

const MzSQLiteContext = createContext<MzSQLite | null>(null)
export const MzSQLiteProvider = MzSQLiteContext.Provider

export function useMzSQLite(): MzSQLite {
  const db = useContext(MzSQLiteContext)
  if (!db) throw new Error('useMzSQLite must be used within <MzSQLiteProvider>')
  return db
}

// ── useLiveQuery ───────────────────────────────────────────────

/**
 * Run a SQL query against local SQLite. Re-evaluates when subscribed
 * tables commit. Returns cached results synchronously.
 */
export function useLiveQuery<T = Record<string, unknown>>(
  sql: string | null,
  params: any[] = [],
  tables?: string[],
): { rows: T[]; version: number } {
  const db = useMzSQLite()
  const [result, setResult] = useState<{ rows: T[]; version: number }>({ rows: [], version: -1 })

  // Track latest sql/params via refs
  const sqlRef = useRef(sql)
  const paramsRef = useRef(params)
  sqlRef.current = sql
  paramsRef.current = params

  // Run query function
  const runQuery = useCallback(async () => {
    const currentSql = sqlRef.current
    if (!currentSql) {
      setResult((prev) => prev.rows.length === 0 ? prev : { rows: [], version: db.getVersion() })
      return
    }
    const rows = await db.queryAsync<T>(currentSql, paramsRef.current)
    setResult({ rows, version: db.getVersion() })
  }, [db])

  // Run on mount and when sql/params change
  useEffect(() => {
    runQuery()
  }, [sql, JSON.stringify(params), runQuery])

  // Subscribe to table commits
  useEffect(() => {
    if (!tables || tables.length === 0) {
      return db.subscribe(() => runQuery())
    }
    const unsubs = tables.map((t) => db.subscribeTable(t, () => runQuery()))
    return () => unsubs.forEach((u) => u())
  }, [db, tables?.join(','), runQuery])

  return result
}

// ── useLiveQueryScalar ─────────────────────────────────────────

export function useLiveQueryScalar<T = number>(
  sql: string | null,
  params: any[] = [],
  tables?: string[],
): T | null {
  const { rows } = useLiveQuery<Record<string, T>>(sql, params, tables)
  if (rows.length === 0) return null
  const first = rows[0]
  const keys = Object.keys(first)
  return keys.length > 0 ? first[keys[0]] : null
}
