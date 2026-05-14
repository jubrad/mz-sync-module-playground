import type { MzEvent, ConnectOpts } from './types.js'

/**
 * A stream of events for a single view.
 * Produced by a `MzTransportProvider` when connecting to a view.
 */
export interface MzViewStream {
  /** Async iterable of subscribe events. Terminates on disconnect. */
  events: AsyncIterable<MzEvent>
  /** Gracefully close the stream. The iterable should terminate. */
  close(): void
}

/**
 * Pluggable transport interface.
 *
 * Implementations connect to a Materialize-compatible event source and
 * produce a stream of typed events. The subscribe client handles
 * reconnection, progress tracking, and batching on top of this.
 *
 * Built-in implementations:
 * - `@mz-web/transport-http` — WebSocket/SSE to mz-http server
 *
 * Future:
 * - `@mz-web/transport-direct` — Materialize native WebSocket subscribe
 */
export interface MzTransportProvider {
  /**
   * Open a stream for a single view.
   *
   * The transport should:
   * 1. Establish a connection (WS, SSE, etc.)
   * 2. Parse wire messages into `MzEvent` objects
   * 3. Yield them as an async iterable
   * 4. Terminate the iterable on disconnect/error
   *
   * The transport should NOT handle reconnection — that's the subscribe
   * client's job.
   */
  connect(view: string, opts: ConnectOpts): MzViewStream
}
