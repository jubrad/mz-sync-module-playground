/** Credentials for authenticating with Materialize. */
export interface MzCredentials {
  /** Email or username. */
  user: string
  /** App password. */
  password: string
}

/** Token-based auth (alternative to user/password). */
export interface MzTokenCredentials {
  /** Access token. */
  token: string
}

export type MzAuth = MzCredentials | MzTokenCredentials

export interface MzDirectTransportOpts {
  /** Materialize host (e.g. "abc123.us-east-1.aws.materialize.cloud" or "localhost:6878"). */
  host: string
  /** Authentication credentials. */
  auth: MzAuth
  /** Use TLS (wss://). Default: true. */
  tls?: boolean
  /** Materialize cluster to use. */
  cluster?: string
  /** Database name. Default: "materialize". */
  database?: string
}

/**
 * Per-view configuration for SUBSCRIBE.
 * The SQL can be a view name or an arbitrary SELECT.
 */
export interface ViewSubscribeConfig {
  /** SQL to subscribe to. Can be a view name or a full SELECT. */
  sql: string
  /** Key column for ENVELOPE UPSERT. Auto-detects if not provided. */
  keyColumn?: string
}

/**
 * Messages from the Materialize WebSocket API.
 */
export type MzWsMessage =
  | { type: 'ReadyForQuery'; payload: { status: string } }
  | { type: 'CommandStarting'; payload: { has_rows: boolean; is_streaming: boolean } }
  | { type: 'Rows'; payload: { columns: MzColumnInfo[] } }
  | { type: 'Row'; payload: unknown[] }
  | { type: 'CommandComplete'; payload: { tag: string } }
  | { type: 'ParameterStatus'; payload: { name: string; value: string } }
  | { type: 'BackendKeyData'; payload: { conn_id: number; secret_key: number } }
  | { type: 'Error'; payload: { message: string; code?: string; severity?: string } }
  | { type: 'Notice'; payload: { message: string; severity?: string } }

export interface MzColumnInfo {
  name: string
  type_oid: number
  type_len: number
  type_mod: number
}
