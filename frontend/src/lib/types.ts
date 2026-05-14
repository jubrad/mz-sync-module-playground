export type MzTimestamp = number

export interface ViewInfo {
  name: string
  keys: number
  hydrated: boolean
  progress_timestamp: MzTimestamp
  title_columns: string[]
}

export interface ViewsResponse {
  server_time_ms: number
  views: ViewInfo[]
}

export interface ChangeEvent {
  type: 'upsert' | 'delete' | 'progress' | 'catchup_complete' | 'snapshot_complete' | 'lagged'
  view?: string
  key?: string
  value?: string
  timestamp?: MzTimestamp
  mode?: string
  events?: number
  keys?: number
  skipped?: number
}

export type Transport = 'sse' | 'ws'

export interface MzStoreOptions {
  transport?: Transport
  reconnectMs?: number
  persist?: boolean
}

// ── Query types ─────────────────────────────────────────────────

export interface QueryOpts<T = Record<string, unknown>> {
  filter?: (row: T, key: string) => boolean
  sort?: (a: T & { _key: string }, b: T & { _key: string }) => number
  limit?: number
  offset?: number
}

export interface QueryResult<T = Record<string, unknown>> {
  rows: (T & { _key: string })[]
  totalCount: number
  filteredCount: number
}
