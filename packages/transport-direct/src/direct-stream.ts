import type { MzEvent, MzTimestamp, ConnectOpts } from '@mz-web/subscribe'
import type { MzViewStream } from '@mz-web/subscribe'
import type { MzAuth, MzDirectTransportOpts, MzWsMessage, MzColumnInfo, ViewSubscribeConfig } from './types.js'

/**
 * Async queue for converting push-based WebSocket to AsyncIterable.
 */
class AsyncQueue<T> implements AsyncIterable<T> {
  private queue: T[] = []
  private waiting: ((result: IteratorResult<T>) => void) | null = null
  private done = false

  push(item: T): void {
    if (this.done) return
    if (this.waiting) {
      const resolve = this.waiting
      this.waiting = null
      resolve({ value: item, done: false })
    } else {
      this.queue.push(item)
    }
  }

  end(): void {
    this.done = true
    if (this.waiting) {
      const resolve = this.waiting
      this.waiting = null
      resolve({ value: undefined as unknown as T, done: true })
    }
  }

  [Symbol.asyncIterator](): AsyncIterator<T> {
    return {
      next: (): Promise<IteratorResult<T>> => {
        if (this.queue.length > 0) {
          return Promise.resolve({ value: this.queue.shift()!, done: false })
        }
        if (this.done) {
          return Promise.resolve({ value: undefined as unknown as T, done: true })
        }
        return new Promise((resolve) => { this.waiting = resolve })
      },
    }
  }
}

/**
 * Quote a SQL identifier, escaping embedded double quotes.
 */
export function quoteIdent(name: string): string {
  return '"' + name.replace(/"/g, '""') + '"'
}

/**
 * Build the SUBSCRIBE SQL for a view with ENVELOPE UPSERT.
 *
 * The subscription always starts with a full snapshot. The view's SQL is a
 * query, and a subscription to a query cannot be resumed from a timestamp,
 * so there is no `AS OF` form.
 */
export function buildSubscribeSQL(
  config: ViewSubscribeConfig,
  cluster?: string,
): string {
  const parts: string[] = []

  if (cluster) {
    parts.push(`SET CLUSTER = ${quoteIdent(cluster)}`)
  }

  const keyCol = config.keyColumn ?? 'key'
  const sql = config.sql

  parts.push(`SUBSCRIBE (${sql}) WITH (SNAPSHOT, PROGRESS) ENVELOPE UPSERT (KEY (${quoteIdent(keyCol)}))`)

  // Join with semicolons for multi-statement simple query
  return parts.join('; ')
}

/**
 * Parse a Row message from a SUBSCRIBE with ENVELOPE UPSERT + PROGRESS.
 *
 * Columns returned by SUBSCRIBE ENVELOPE UPSERT with PROGRESS:
 *   [mz_timestamp, mz_progressed, mz_state, key_col, ...value_cols]
 *
 * When mz_progressed is "t" (true), it's a progress message.
 * When mz_state is "upsert", it's an insert/update.
 * When mz_state is "delete", it's a delete.
 */
export function parseSubscribeRow(
  row: unknown[],
  columns: MzColumnInfo[],
): MzEvent | null {
  if (row.length < 3) return null

  const mzTimestamp = Number(row[0]) as MzTimestamp
  const mzProgressed = row[1]

  // Progress message
  if (mzProgressed === 't' || mzProgressed === true || mzProgressed === 'true') {
    return { type: 'progress', timestamp: mzTimestamp }
  }

  // W6: bounds check — need at least 4 elements for non-progress rows
  if (row.length < 4) return null

  const mzState = String(row[2])
  const key = String(row[3])

  if (mzState === 'upsert') {
    // Build value object from remaining columns
    // Columns: [mz_timestamp, mz_progressed, mz_state, key_col, ...value_cols]
    const value: Record<string, unknown> = {}
    for (let i = 3; i < columns.length; i++) {
      const colName = columns[i].name
      value[colName] = row[i]
    }
    return { type: 'upsert', key, value, timestamp: mzTimestamp }
  }

  if (mzState === 'delete') {
    return { type: 'delete', key, timestamp: mzTimestamp }
  }

  return null // key_violation or unknown state
}

/**
 * Tracks where the initial snapshot of a SUBSCRIBE ends.
 *
 * The snapshot rows all carry the stream's first timestamp, and a progress
 * row at that timestamp can arrive before them. The snapshot is complete at
 * the first progress row past it, which becomes `snapshot_complete`. Progress
 * rows before that are dropped.
 */
export function snapshotBoundary(): (event: MzEvent) => MzEvent | null {
  let asOf: MzTimestamp | null = null
  let inSnapshot = true

  return (event) => {
    if (event.type !== 'progress') {
      if (asOf === null && 'timestamp' in event && event.timestamp !== undefined) {
        asOf = event.timestamp
      }
      return event
    }
    if (!inSnapshot) return event
    if (asOf === null) asOf = event.timestamp
    if (event.timestamp <= asOf) return null
    inSnapshot = false
    return { type: 'snapshot_complete', timestamp: event.timestamp }
  }
}

/**
 * Create a direct WebSocket stream to Materialize for a single SUBSCRIBE.
 *
 * `connectOpts.since` is ignored: every connection takes a full snapshot and
 * ends it with `snapshot_complete`, which the subscribe client reconciles
 * against the keys it already holds.
 */
export function createDirectStream(
  transportOpts: MzDirectTransportOpts,
  viewConfig: ViewSubscribeConfig,
  _connectOpts: ConnectOpts,
): MzViewStream {
  const queue = new AsyncQueue<MzEvent>()
  let ws: WebSocket | null = null
  let columns: MzColumnInfo[] = []
  let authenticated = false
  const boundary = snapshotBoundary()

  const proto = (transportOpts.tls ?? true) ? 'wss' : 'ws'
  const url = `${proto}://${transportOpts.host}/api/experimental/sql`

  ws = new WebSocket(url)

  ws.onopen = () => {
    // Send authentication
    const auth: Record<string, unknown> = 'token' in transportOpts.auth
      ? { token: (transportOpts.auth as { token: string }).token }
      : { user: (transportOpts.auth as { user: string; password: string }).user,
          password: (transportOpts.auth as { user: string; password: string }).password }

    if (transportOpts.database) {
      auth.options = { database: transportOpts.database }
    }

    ws!.send(JSON.stringify(auth))
  }

  ws.onmessage = (e) => {
    let msg: MzWsMessage
    try {
      msg = JSON.parse(e.data)
    } catch {
      return
    }

    switch (msg.type) {
      case 'ReadyForQuery':
        if (!authenticated) {
          // Auth complete — send SUBSCRIBE
          authenticated = true
          const sql = buildSubscribeSQL(viewConfig, transportOpts.cluster)
          ws!.send(JSON.stringify({ query: sql }))
        }
        break

      case 'Rows':
        // Column metadata for the SUBSCRIBE result
        columns = msg.payload.columns
        break

      case 'Row': {
        const parsed = parseSubscribeRow(msg.payload, columns)
        const event = parsed && boundary(parsed)
        if (event) queue.push(event)
        break
      }

      case 'CommandStarting':
        // Streaming is starting
        break

      case 'Error':
        console.error('[transport-direct] MZ error:', msg.payload.message)
        queue.end()
        break

      case 'Notice':
        // Ignore notices
        break
    }
  }

  ws.onclose = () => {
    queue.end()
  }

  ws.onerror = () => {
    // W5: Close the WebSocket on error to ensure cleanup
    ws?.close()
    queue.end()
  }

  return {
    events: queue,
    close() {
      ws?.close()
      queue.end()
    },
  }
}
