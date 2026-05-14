import type { MzEvent, ConnectOpts } from '@mz-web/subscribe'
import type { MzViewStream } from '@mz-web/subscribe'
import { AsyncQueue } from './async-queue.js'

/**
 * Create a WebSocket-based MzViewStream for a view.
 */
export function createWsStream(baseUrl: string, view: string, opts: ConnectOpts): MzViewStream {
  const queue = new AsyncQueue<MzEvent>()

  // Derive WebSocket URL
  let wsBase: string
  if (!baseUrl || baseUrl === '') {
    const loc = globalThis.location
    wsBase = `${loc.protocol === 'https:' ? 'wss' : 'ws'}://${loc.host}`
  } else {
    const proto = baseUrl.startsWith('https') ? 'wss' : 'ws'
    const host = baseUrl.replace(/^https?:\/\//, '')
    wsBase = `${proto}://${host}`
  }

  const params = new URLSearchParams()
  if (opts.since && opts.since > 0) {
    params.set('since', String(opts.since))
  } else if (opts.snapshot) {
    params.set('snapshot', 'true')
  }

  const url = `${wsBase}/api/ws/${encodeURIComponent(view)}?${params}`
  const ws = new WebSocket(url)

  ws.onmessage = (e) => {
    try {
      const msg = JSON.parse(e.data) as MzEvent
      // Parse string values into objects
      if (msg.type === 'upsert' && typeof msg.value === 'string') {
        try {
          (msg as any).value = JSON.parse(msg.value)
        } catch {
          // keep as string
        }
      }
      queue.push(msg)
    } catch {
      // malformed message, skip
    }
  }

  ws.onclose = () => queue.end()
  ws.onerror = () => queue.end()

  return {
    events: queue,
    close() {
      ws.close()
      queue.end()
    },
  }
}
