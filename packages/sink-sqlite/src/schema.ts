import type { ColumnDef, SqliteType, TableSchema, ServerSchemaResponse, SchemaMode } from './types.js'

/** Map Postgres wire types to SQLite column types. */
export function pgTypeToSqlite(pgType: string): SqliteType {
  switch (pgType) {
    case 'int2': case 'int4': case 'int8':
    case 'oid': case 'bool':
      return 'integer'
    case 'float4': case 'float8': case 'numeric':
      return 'real'
    default:
      return 'text'
  }
}

/** Infer SQLite column type from a JS value. */
export function inferTypeFromValue(val: unknown): SqliteType {
  if (typeof val === 'number') return Number.isInteger(val) ? 'integer' : 'real'
  if (typeof val === 'boolean') return 'integer'
  return 'text'
}

/** Infer a table schema from a row object. */
export function inferSchema(row: Record<string, unknown>, primaryKey?: string): TableSchema {
  const pk = primaryKey || 'id'
  const columns: ColumnDef[] = []

  // Ensure PK is first
  if (pk in row) {
    columns.push({ name: pk, type: inferTypeFromValue(row[pk]) })
  } else {
    columns.push({ name: pk, type: 'text' })
  }

  for (const [key, val] of Object.entries(row)) {
    if (key === pk) continue
    columns.push({ name: key, type: inferTypeFromValue(val) })
  }

  return { columns, primaryKey: pk }
}

/** Build a TableSchema from an explicit column map. */
export function explicitSchema(
  columnMap: Record<string, SqliteType>,
  primaryKey?: string,
): TableSchema {
  const pk = primaryKey || Object.keys(columnMap)[0] || 'id'
  const columns: ColumnDef[] = Object.entries(columnMap).map(([name, type]) => ({
    name,
    type,
  }))
  return { columns, primaryKey: pk }
}

/** Fetch schema from the mz-http server. */
export async function fetchServerSchema(
  baseUrl: string,
  view: string,
): Promise<TableSchema | null> {
  try {
    const r = await fetch(`${baseUrl}/api/views/${encodeURIComponent(view)}/schema`)
    if (!r.ok) return null
    const data: ServerSchemaResponse = await r.json()
    return {
      columns: data.columns.map((c) => ({
        name: c.name,
        type: pgTypeToSqlite(c.pg_type),
      })),
      primaryKey: data.key_column,
    }
  } catch {
    return null
  }
}

/** Resolve a schema based on the mode. Returns null if deferred (infer mode). */
export async function resolveSchema(
  view: string,
  mode: SchemaMode | undefined,
  serverBaseUrl?: string,
): Promise<TableSchema | null> {
  if (!mode || mode === 'infer') return null

  if (mode === 'server') {
    if (serverBaseUrl === undefined || serverBaseUrl === null) {
      console.warn(`[sink-sqlite] schema='server' but no serverBaseUrl provided for ${view}`)
      return null
    }
    return fetchServerSchema(serverBaseUrl, view)
  }

  // Explicit mode
  return explicitSchema(mode.columns, mode.primaryKey)
}
