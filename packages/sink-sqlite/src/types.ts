/**
 * Minimal interface any SQLite implementation must provide.
 * Implemented by @mz-web/sqlite-wasm for browsers, or by
 * better-sqlite3, Bun's sqlite, etc. for other runtimes.
 */
export interface SqliteExec {
  /** Execute one or more SQL statements (DDL/DML). */
  exec(sql: string): Promise<void>
  /** Execute a query and return rows as objects. */
  query<T = Record<string, unknown>>(sql: string, params?: unknown[]): Promise<T[]>
}

export type SqliteType = 'text' | 'integer' | 'real'

export interface ColumnDef {
  name: string
  type: SqliteType
}

export interface TableSchema {
  columns: ColumnDef[]
  primaryKey: string
}

/** Schema from the mz-http /api/views/:name/schema endpoint. */
export interface ServerSchemaResponse {
  name: string
  key_column: string
  columns: { name: string; pg_type: string }[]
}

export type SchemaMode =
  | 'server'   // fetch from mz-http /api/views/:name/schema
  | 'infer'    // infer from first upsert row
  | { columns: Record<string, SqliteType>; primaryKey?: string }  // explicit

export interface SinkSqliteOpts {
  /** The SQLite database to write to. */
  db: SqliteExec
  /** Schema discovery mode (default: 'infer'). */
  schema?: SchemaMode
  /** Base URL for server schema discovery (required when schema='server'). */
  serverBaseUrl?: string
  /** Called after each batch is committed. */
  onCommit?(view: string, version: number): void
}
