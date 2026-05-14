/** Metadata for a single Materialize view exposed by the HTTP API. */
export interface ViewInfo {
  name: string
  keys: string[]
  hydrated: boolean
  progress_timestamp: number
  title_columns: string[]
}

/** Response shape from GET /api/views. */
export interface ViewsResponse {
  views: ViewInfo[]
  server_time_ms: number
}
