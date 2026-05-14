/**
 * MzTransport — pure SSE/WS transport layer.
 * Connects, reconnects, parses messages, and forwards events via callbacks.
 * No state management — that's the store's job.
 */
import type { ChangeEvent, MzTimestamp, Transport } from './types'

export interface TransportCallbacks {
  onEvent(event: ChangeEvent): void
  onConnected(): void
  onDisconnected(): void
  getProgress(): MzTimestamp
}

export class MzTransport {
  private baseUrl: string
  private viewName: string
  private transport: Transport
  private reconnectMs: number
  private callbacks: TransportCallbacks

  private conn: EventSource | WebSocket | null = null
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null
  private destroyed = false

  connected = false

  constructor(
    baseUrl: string,
    viewName: string,
    callbacks: TransportCallbacks,
    opts?: { transport?: Transport; reconnectMs?: number },
  ) {
    this.baseUrl = baseUrl.replace(/\/$/, '')
    this.viewName = viewName
    this.callbacks = callbacks
    this.transport = opts?.transport || 'ws'
    this.reconnectMs = opts?.reconnectMs ?? 2000
  }

  connect() {
    this.destroyed = false
    this.doConnect()
  }

  disconnect() {
    this.destroyed = true
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer)
    this.closeConn()
  }

  // ── Connection ────────────────────────────────────

  private doConnect() {
    if (this.destroyed) return
    this.closeConn()

    const params = new URLSearchParams()
    const progress = this.callbacks.getProgress()
    if (progress > 0) {
      params.set('since', String(progress))
    } else {
      params.set('snapshot', 'true')
    }

    if (this.transport === 'ws') {
      this.connectWS(params)
    } else {
      this.connectSSE(params)
    }
  }

  private connectSSE(params: URLSearchParams) {
    const url = `${this.baseUrl}/api/sse/${encodeURIComponent(this.viewName)}?${params}`
    const es = new EventSource(url)
    this.conn = es

    es.onopen = () => {
      this.connected = true
      this.callbacks.onConnected()
    }
    es.onerror = () => {
      this.connected = false
      this.callbacks.onDisconnected()
      this.scheduleReconnect()
    }

    for (const evType of ['upsert', 'delete', 'progress', 'catchup_complete', 'snapshot_complete']) {
      es.addEventListener(evType, (e) => this.handleMessage((e as MessageEvent).data))
    }
  }

  private connectWS(params: URLSearchParams) {
    let base = this.baseUrl
    if (!base) {
      // Same-origin: derive from current page
      const loc = window.location
      base = `${loc.protocol}//${loc.host}`
    }
    const proto = base.startsWith('https') ? 'wss' : 'ws'
    const host = base.replace(/^https?:\/\//, '')
    const url = `${proto}://${host}/api/ws/${encodeURIComponent(this.viewName)}?${params}`
    const ws = new WebSocket(url)
    this.conn = ws

    ws.onopen = () => {
      this.connected = true
      this.callbacks.onConnected()
    }
    ws.onclose = () => {
      this.connected = false
      this.callbacks.onDisconnected()
      this.scheduleReconnect()
    }
    ws.onerror = () => {}
    ws.onmessage = (e) => this.handleMessage(e.data)
  }

  private handleMessage(rawData: string) {
    let msg: ChangeEvent
    try { msg = JSON.parse(rawData) } catch { return }
    this.callbacks.onEvent(msg)
  }

  private closeConn() {
    if (this.conn) {
      this.conn.close()
      this.conn = null
    }
    this.connected = false
  }

  private scheduleReconnect() {
    if (this.destroyed) return
    this.closeConn()
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer)
    this.reconnectTimer = setTimeout(() => this.doConnect(), this.reconnectMs)
  }
}
