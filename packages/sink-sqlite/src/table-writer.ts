import type { TableSchema } from './types.js'

/** Escape a value for inline SQL. */
export function escapeVal(v: unknown): string {
  if (v === null || v === undefined) return 'NULL'
  if (typeof v === 'number') return String(v)
  if (typeof v === 'boolean') return v ? '1' : '0'
  // Objects/arrays — serialize to JSON string
  if (typeof v === 'object') return `'${JSON.stringify(v).replace(/'/g, "''")}'`
  return `'${String(v).replace(/'/g, "''")}'`
}

/** Generate CREATE TABLE IF NOT EXISTS DDL. */
export function createTableDDL(view: string, schema: TableSchema): string {
  const colDefs = schema.columns
    .map((c) => `"${c.name}" ${c.type.toUpperCase()}`)
    .join(', ')
  return `CREATE TABLE IF NOT EXISTS "${view}" (${colDefs}, PRIMARY KEY ("${schema.primaryKey}"))`
}

/**
 * Generate a batch SQL string: BEGIN; INSERT OR REPLACE...; DELETE...; COMMIT;
 *
 * All values are inline-escaped (no parameter binding) for compatibility
 * with the widest range of SQLite interfaces.
 */
export function batchWriteSQL(
  view: string,
  schema: TableSchema,
  upserts: Map<string, Record<string, unknown>>,
  deletes: Set<string>,
): string {
  const stmts: string[] = ['BEGIN']

  for (const [key, value] of upserts) {
    const vals = schema.columns.map((col) => {
      const v = col.name === schema.primaryKey ? key : value[col.name]
      return escapeVal(v)
    })
    const colNames = schema.columns.map((c) => `"${c.name}"`).join(',')
    stmts.push(`INSERT OR REPLACE INTO "${view}" (${colNames}) VALUES (${vals.join(',')})`)
  }

  for (const key of deletes) {
    stmts.push(`DELETE FROM "${view}" WHERE "${schema.primaryKey}" = ${escapeVal(key)}`)
  }

  stmts.push('COMMIT')
  return stmts.join(';\n')
}
