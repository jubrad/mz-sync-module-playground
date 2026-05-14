import type { MzEvent, ConnectOpts } from '@mz-web/subscribe'
import type { MzViewStream } from '@mz-web/subscribe'
import { AsyncQueue } from './async-queue.js'

const SSE_EVENT_TYPES = ['upsert', 'delete', 'progress', 'catchup_complete', 'snapshot_complete'] as const

/**
 * Create an SSE-based MzViewStream for a view.
 */
export function createSseStream(baseUrl: string, view: string, opts: ConnectOpts): MzViewStream {
  const queue = new AsyncQueue<MzEvent>()

  const resolvedBase = baseUrl || ''
  const params = new URLSearchParams()
  if (opts.since && opts.since > 0) {
    params.set('since', String(opts.since))
  } else if (opts.snapshot) {
    params.set('snapshot', 'true')
  }

  const url = `${resolvedBase}/api/sse/${encodeURIComponent(view)}?${params}`
  const es = new EventSource(url)

  for (const evType of SSE_EVENT_TYPES) {
    es.addEventListener(evType, (e) => {
      try {
        const msg = JSON.parse((e as MessageEvent).data) as MzEvent
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
    })
  }

  es.onerror = () => {
    es.close()
    queue.end()
  }

  return {
    events: queue,
    close() {
      es.close()
      queue.end()
    },
  }
}
