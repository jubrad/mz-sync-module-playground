import { useState, useEffect, useCallback, useRef } from 'react'
import { useMzContext } from './context.js'

export interface UseLiveQueryResult<T> {
  rows: T[]
  version: number
}

/**
 * Run a SQL query against the local SQLite sink and re-run it whenever
 * the subscribed tables receive new commits.
 *
 * When `sql` is null the hook is inactive and returns empty rows.
 *
 * @param sql    - SQL query string, or null to disable
 * @param params - Bind parameters for the query
 * @param tables - View/table names whose commits trigger a re-query
 */
export function useLiveQuery<T = Record<string, unknown>>(
  sql: string | null,
  params: unknown[] = [],
  tables: string[] = [],
): UseLiveQueryResult<T> {
  const { sqliteSink } = useMzContext()
  const [rows, setRows] = useState<T[]>([])
  const [version, setVersion] = useState(0)

  // Keep a stable ref to the latest params to avoid re-subscriptions
  const paramsRef = useRef(params)
  paramsRef.current = params

  // C1: Serialize params so we can use it as a dependency
  const paramsKey = JSON.stringify(params)

  // C3: Request ID ref to prevent stale subscription results from overwriting fresh ones
  const requestIdRef = useRef(0)

  const runQuery = useCallback(async () => {
    const myId = ++requestIdRef.current
    if (!sql || !sqliteSink) {
      setRows([])
      return
    }
    try {
      const result = await sqliteSink.query<T>(sql, paramsRef.current)
      if (myId === requestIdRef.current) {
        setRows(result)
        setVersion((v) => v + 1)
      }
    } catch (e) {
      if (myId === requestIdRef.current) {
        console.error('[useLiveQuery] query error:', e)
      }
    }
  }, [sql, sqliteSink])

  // C1 + C2: Run query initially and when sql, sqliteSink, or params change.
  // The cancelled flag prevents setState after unmount or after dependencies change.
  useEffect(() => {
    let cancelled = false
    const myId = ++requestIdRef.current

    const run = async () => {
      if (!sql || !sqliteSink) {
        if (!cancelled) setRows([])
        return
      }
      try {
        const result = await sqliteSink.query<T>(sql, paramsRef.current)
        if (!cancelled && myId === requestIdRef.current) {
          setRows(result)
          setVersion((v) => v + 1)
        }
      } catch (e) {
        if (!cancelled) console.error('[useLiveQuery]', e)
      }
    }
    run()
    return () => {
      cancelled = true
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sql, sqliteSink, paramsKey])

  // W1: Use JSON.stringify(tables) instead of tables.join(',') for safety
  const tablesKey = JSON.stringify(tables)

  // Subscribe to table commits and re-run query
  useEffect(() => {
    if (!sql || !sqliteSink || tables.length === 0) return

    const unsubs = tables.map((table) =>
      sqliteSink.subscribeTable(table, () => {
        runQuery()
      }),
    )

    return () => {
      for (const unsub of unsubs) unsub()
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [sql, sqliteSink, tablesKey, runQuery])

  return { rows, version }
}
