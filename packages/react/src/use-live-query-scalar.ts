import { useMemo } from 'react'
import { useLiveQuery } from './use-live-query.js'

/**
 * Convenience wrapper around useLiveQuery that extracts the first column
 * of the first row, useful for aggregate queries like `SELECT count(*)`.
 *
 * Returns `T | null` directly (not wrapped in an object).
 *
 * @param sql    - SQL query string, or null to disable
 * @param params - Bind parameters
 * @param tables - View/table names whose commits trigger a re-query
 */
export function useLiveQueryScalar<T = number>(
  sql: string | null,
  params: unknown[] = [],
  tables: string[] = [],
): T | null {
  const { rows } = useLiveQuery<Record<string, unknown>>(sql, params, tables)

  return useMemo(() => {
    if (rows.length === 0) return null
    const firstRow = rows[0]
    const keys = Object.keys(firstRow)
    if (keys.length === 0) return null
    return firstRow[keys[0]] as T
  }, [rows])
}
