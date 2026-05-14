import type { MzTransportProvider, MzViewStream, ConnectOpts } from '@mz-web/subscribe'
import { createWsStream } from './ws-stream.js'
import { createSseStream } from './sse-stream.js'

export interface MzHttpTransportOpts {
  /** Base URL of the mz-http server (e.g. "http://localhost:8080"). Empty string = same origin. */
  baseUrl: string
  /** Transport mechanism: 'ws' (default) or 'sse'. */
  mode?: 'ws' | 'sse'
}

/**
 * Transport provider for the mz-http server.
 *
 * Connects via WebSocket (default) or SSE to the mz-http REST API.
 * Each `connect()` call opens a new connection for one view.
 */
export class MzHttpTransport implements MzTransportProvider {
  private baseUrl: string
  private mode: 'ws' | 'sse'

  constructor(opts: MzHttpTransportOpts) {
    this.baseUrl = opts.baseUrl.replace(/\/$/, '')
    this.mode = opts.mode ?? 'ws'
  }

  connect(view: string, opts: ConnectOpts): MzViewStream {
    if (this.mode === 'sse') {
      return createSseStream(this.baseUrl, view, opts)
    }
    return createWsStream(this.baseUrl, view, opts)
  }
}
