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
 */
export function buildSubscribeSQL(
  config: ViewSubscribeConfig,
  opts: ConnectOpts,
  cluster?: string,
): string {
  const parts: string[] = []

  if (cluster) {
    parts.push(`SET CLUSTER = ${quoteIdent(cluster)}`)
  }

  const keyCol = config.keyColumn ?? 'key'
  const sql = config.sql

  if (opts.since && opts.since > 0) {
    // C4: Validate AS OF value
    if (!Number.isSafeInteger(opts.since)) {
      throw new Error('Invalid since timestamp')
    }
    parts.push(`SUBSCRIBE (${sql}) WITH (PROGRESS) AS OF ${opts.since} ENVELOPE UPSERT (KEY (${quoteIdent(keyCol)}))`)
  } else {
    parts.push(`SUBSCRIBE (${sql}) WITH (SNAPSHOT, PROGRESS) ENVELOPE UPSERT (KEY (${quoteIdent(keyCol)}))`)
  }

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
 * Create a direct WebSocket stream to Materialize for a single SUBSCRIBE.
 */
export function createDirectStream(
  transportOpts: MzDirectTransportOpts,
  viewConfig: ViewSubscribeConfig,
  connectOpts: ConnectOpts,
): MzViewStream {
  const queue = new AsyncQueue<MzEvent>()
  let ws: WebSocket | null = null
  let columns: MzColumnInfo[] = []
  let authenticated = false
  // W3: When reconnecting with since > 0, we are not in snapshot mode
  let inSnapshot = !(connectOpts.since && connectOpts.since > 0)
  let firstProgress = true

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
          const sql = buildSubscribeSQL(viewConfig, connectOpts, transportOpts.cluster)
          ws!.send(JSON.stringify({ query: sql }))
        }
        break

      case 'Rows':
        // Column metadata for the SUBSCRIBE result
        columns = msg.payload.columns
        break

      case 'Row': {
        const event = parseSubscribeRow(msg.payload, columns)
        if (event) {
          // W2: Emit snapshot_complete INSTEAD of the progress event when in snapshot
          if (event.type === 'progress' && inSnapshot) {
            inSnapshot = false
            firstProgress = false
            queue.push({ type: 'snapshot_complete', timestamp: event.timestamp })
          } else if (event.type === 'progress' && firstProgress) {
            firstProgress = false
            // W3: For catchup (since > 0), emit catchup_complete on first progress
            if (connectOpts.since && connectOpts.since > 0) {
              queue.push({ type: 'catchup_complete', timestamp: event.timestamp })
            } else {
              queue.push(event)
            }
          } else {
            queue.push(event)
          }
        }
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
