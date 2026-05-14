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

  const runQuery = useCallback(async () => {
    if (!sql || !sqliteSink) {
      setRows([])
      return
    }
    try {
      const result = await sqliteSink.query<T>(sql, paramsRef.current)
      setRows(result)
      setVersion((v) => v + 1)
    } catch (e) {
      console.error('[useLiveQuery] query error:', e)
    }
  }, [sql, sqliteSink])

  // Run query initially and on sql change
  useEffect(() => {
    runQuery()
  }, [runQuery])

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
  }, [sql, sqliteSink, tables.join(','), runQuery])

  return { rows, version }
}
