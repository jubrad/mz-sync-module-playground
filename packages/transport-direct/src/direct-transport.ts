import type { MzTransportProvider, MzViewStream, ConnectOpts } from '@mz-web/subscribe'
import type { MzDirectTransportOpts, ViewSubscribeConfig } from './types.js'
import { createDirectStream } from './direct-stream.js'

/**
 * Direct transport to Materialize via its native WebSocket API.
 *
 * Connects to wss://<host>/api/experimental/sql, authenticates, and sends
 * SUBSCRIBE queries with ENVELOPE UPSERT. No mz-http proxy needed.
 *
 * Each `connect()` call opens a new WebSocket connection for one view.
 *
 * Usage:
 * ```typescript
 * const transport = new MzDirectTransport({
 *   host: 'abc123.us-east-1.aws.materialize.cloud',
 *   auth: { user: 'you@example.com', password: 'mzp_...' },
 *   cluster: 'default',
 * })
 * ```
 *
 * Views must be registered with their SQL and key column before connecting:
 * ```typescript
 * transport.registerView('live_orders', {
 *   sql: 'SELECT * FROM live_orders',
 *   keyColumn: 'id',
 * })
 * ```
 */
export class MzDirectTransport implements MzTransportProvider {
  private opts: MzDirectTransportOpts
  private viewConfigs = new Map<string, ViewSubscribeConfig>()

  constructor(opts: MzDirectTransportOpts) {
    this.opts = opts
  }

  /**
   * Register a view's SUBSCRIBE configuration.
   *
   * Must be called before `connect()` for that view. The SQL can be a
   * view name or an arbitrary SELECT statement.
   */
  registerView(view: string, config: ViewSubscribeConfig): this {
    this.viewConfigs.set(view, config)
    return this
  }

  /**
   * Register multiple views at once.
   */
  registerViews(views: Record<string, ViewSubscribeConfig>): this {
    for (const [name, config] of Object.entries(views)) {
      this.viewConfigs.set(name, config)
    }
    return this
  }

  connect(view: string, opts: ConnectOpts): MzViewStream {
    const config = this.viewConfigs.get(view)
    if (!config) {
      throw new Error(
        `[transport-direct] No config registered for view "${view}". ` +
        `Call transport.registerView("${view}", { sql: "...", keyColumn: "..." }) first.`
      )
    }
    return createDirectStream(this.opts, config, opts)
  }
}
