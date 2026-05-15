import { BatchAccumulator } from './batch.js'
import type { MzTransportProvider, MzViewStream } from './transport.js'
import type { MzBatch, MzTimestamp, ViewHandle } from './types.js'

/**
 * Callback-based sink interface.
 * Register sinks with the subscribe client to receive batches.
 */
export interface MzSink {
  /** Called with a committed batch of changes. */
  onBatch(batch: MzBatch): void | Promise<void>
  /** Called when connection state changes for a view. */
  onConnectionChange?(view: string, connected: boolean): void
}

export interface MzSubscribeOpts {
  /** Transport provider for connecting to views. */
  transport: MzTransportProvider
  /** Initial views to subscribe to. */
  views?: string[]
  /** Reconnection delay in ms (default 2000). */
  reconnectMs?: number
  /** Maximum reconnection delay in ms (default 60000). */
  maxReconnectMs?: number
}

interface ViewState {
  view: string
  progress: MzTimestamp
  ready: boolean
  connected: boolean
  refCount: number
  stream: MzViewStream | null
  abortController: AbortController | null
  reconnectTimer: ReturnType<typeof setTimeout> | null
  backoff: number
  /** Generation counter — incremented on resync/teardown to detect stale consume loops. */
  generation: number
}

/**
 * Core subscribe client.
 *
 * Manages subscriptions to Materialize views via a pluggable transport.
 * Events are batched between progress boundaries and delivered to
 * registered sinks as atomic `MzBatch` objects.
 *
 * Handles:
 * - Progress cursor tracking per view
 * - Reconnection with exponential backoff
 * - Snapshot vs changelog catchup decisions
 * - Ref-counted view lifecycle
 */
export class MzSubscribeClient {
  private transport: MzTransportProvider
  private reconnectMs: number
  private maxReconnectMs: number
  private views = new Map<string, ViewState>()
  private sinks = new Set<MzSink>()
  private destroyed = false

  constructor(opts: MzSubscribeOpts) {
    this.transport = opts.transport
    this.reconnectMs = opts.reconnectMs ?? 2000
    this.maxReconnectMs = opts.maxReconnectMs ?? 60000

    if (opts.views) {
      for (const v of opts.views) this.addView(v)
    }
  }

  /** Register a sink to receive batches. Returns unsubscribe function. */
  addSink(sink: MzSink): () => void {
    this.sinks.add(sink)
    return () => this.sinks.delete(sink)
  }

  /** Start subscribing to a view. Ref-counted — safe to call multiple times. */
  addView(view: string): ViewHandle {
    if (this.destroyed) {
      throw new Error(`[mz-subscribe] cannot addView("${view}"): client is destroyed`)
    }

    let state = this.views.get(view)
    if (state) {
      state.refCount++
      return this.viewHandle(state)
    }

    state = {
      view,
      progress: 0,
      ready: false,
      connected: false,
      refCount: 1,
      stream: null,
      abortController: null,
      reconnectTimer: null,
      backoff: this.reconnectMs,
      generation: 0,
    }
    this.views.set(view, state)
    this.startConsuming(state)
    return this.viewHandle(state)
  }

  /** Decrement ref count for a view. Tears down when it hits 0. */
  removeView(view: string): void {
    const state = this.views.get(view)
    if (!state) return
    state.refCount--
    if (state.refCount <= 0) {
      this.teardownView(state)
      this.views.delete(view)
    }
  }

  /** Get a handle for a view. */
  getView(view: string): ViewHandle | undefined {
    const state = this.views.get(view)
    return state ? this.viewHandle(state) : undefined
  }

  /** Get handles for all active views. */
  getViews(): ViewHandle[] {
    return [...this.views.values()].map((s) => this.viewHandle(s))
  }

  /** Force full resync of all views. Drops progress, reconnects with snapshot. */
  resync(): void {
    for (const state of this.views.values()) {
      state.progress = 0
      state.ready = false
      state.generation++
      this.teardownView(state)
      this.startConsuming(state)
    }
  }

  /** Tear down all connections. */
  destroy(): void {
    this.destroyed = true
    for (const state of this.views.values()) {
      this.teardownView(state)
    }
    this.views.clear()
    this.sinks.clear()
  }

  // ── Internal ───────────────────────────────────────

  private viewHandle(state: ViewState): ViewHandle {
    return {
      get view() { return state.view },
      get progress() { return state.progress },
      get ready() { return state.ready },
      get connected() { return state.connected },
    }
  }

  private teardownView(state: ViewState): void {
    if (state.reconnectTimer) {
      clearTimeout(state.reconnectTimer)
      state.reconnectTimer = null
    }
    if (state.abortController) {
      state.abortController.abort()
      state.abortController = null
    }
    if (state.stream) {
      state.stream.close()
      state.stream = null
    }
    state.connected = false
  }

  private async startConsuming(state: ViewState): Promise<void> {
    if (this.destroyed) return

    const generation = state.generation
    const abort = new AbortController()
    state.abortController = abort

    try {
      const opts = state.progress > 0
        ? { since: state.progress }
        : { snapshot: true }

      const stream = this.transport.connect(state.view, opts)
      state.stream = stream
      state.connected = true
      state.backoff = this.reconnectMs
      this.notifyConnectionChange(state.view, true)

      const accumulator = new BatchAccumulator(state.view)
      if (!state.ready) accumulator.setSnapshotMode(true)

      for await (const event of stream.events) {
        if (abort.signal.aborted || state.generation !== generation) break

        switch (event.type) {
          case 'upsert':
          case 'delete':
            accumulator.add(event)
            break

          case 'progress':
            if (accumulator.hasPending) {
              const batch = accumulator.seal(event.timestamp)
              state.progress = event.timestamp
              if (state.generation !== generation) break
              await this.deliverBatch(batch)
            } else {
              state.progress = event.timestamp
            }
            break

          case 'snapshot_complete':
            if (accumulator.hasPending) {
              const batch = accumulator.seal(event.timestamp, 'snapshot')
              state.progress = event.timestamp
              if (state.generation !== generation) break
              await this.deliverBatch(batch)
            } else {
              // W3: advance progress even when accumulator has no pending events
              state.progress = event.timestamp
            }
            state.ready = true
            break

          case 'catchup_complete':
            if (accumulator.hasPending) {
              const batch = accumulator.seal(event.timestamp, 'catchup')
              state.progress = event.timestamp
              if (state.generation !== generation) break
              await this.deliverBatch(batch)
            } else {
              // W3: advance progress even when accumulator has no pending events
              state.progress = event.timestamp
            }
            state.ready = true
            break

          case 'lagged':
            // Server says our cursor is too old — reset and reconnect
            state.progress = 0
            state.ready = false
            // W4: clear the accumulator and break to force reconnect with fresh snapshot
            accumulator.seal(0)
            break
        }

        // W4: after lagged, break out of the for-await loop to trigger reconnect
        if (event.type === 'lagged') break
      }
    } catch {
      // Stream errored — will reconnect below
    }

    // If generation changed, another startConsuming is already running — bail out silently
    if (state.generation !== generation) return

    // Stream ended (disconnect, error, or abort)
    state.connected = false
    state.stream = null
    state.abortController = null
    this.notifyConnectionChange(state.view, false)

    // Reconnect unless destroyed or explicitly torn down
    if (!this.destroyed && this.views.has(state.view) && state.refCount > 0) {
      this.scheduleReconnect(state)
    }
  }

  private scheduleReconnect(state: ViewState): void {
    if (state.reconnectTimer) return
    const delay = state.backoff * (0.5 + Math.random()) // W6: jitter
    state.backoff = Math.min(state.backoff * 1.5, this.maxReconnectMs)
    const gen = state.generation
    state.reconnectTimer = setTimeout(() => {
      state.reconnectTimer = null
      // W5: if generation changed while we were waiting, abort silently
      if (state.generation !== gen) return
      this.startConsuming(state)
    }, delay)
  }

  private async deliverBatch(batch: MzBatch): Promise<void> {
    for (const sink of this.sinks) {
      try {
        await sink.onBatch(batch)
      } catch (e) {
        console.error(`[mz-subscribe] sink error for ${batch.view}:`, e)
      }
    }
  }

  private notifyConnectionChange(view: string, connected: boolean): void {
    for (const sink of this.sinks) {
      try {
        sink.onConnectionChange?.(view, connected)
      } catch {
        // ignore
      }
    }
  }
}
