import { useState, useEffect, useRef } from 'react'
import { useMzContext } from './context.js'
import type { ViewInfo, ViewsResponse } from './types.js'

export interface UseMzViewsResult {
  views: ViewInfo[]
  serverTimeMs: number
  latencyMs: number
  loading: boolean
}

/**
 * Polls the mz-http /api/views endpoint at a regular interval.
 *
 * @param intervalMs - Polling interval in milliseconds (default 3000)
 */
export function useMzViews(intervalMs = 3000): UseMzViewsResult {
  const { baseUrl } = useMzContext()
  const [views, setViews] = useState<ViewInfo[]>([])
  const [serverTimeMs, setServerTimeMs] = useState(0)
  const [latencyMs, setLatencyMs] = useState(0)
  const [loading, setLoading] = useState(true)
  const mountedRef = useRef(true)

  useEffect(() => {
    mountedRef.current = true

    async function fetchViews() {
      const start = Date.now()
      try {
        const res = await fetch(`${baseUrl}/api/views`)
        if (!res.ok) return
        const data: ViewsResponse = await res.json()
        if (!mountedRef.current) return
        setViews(data.views)
        setServerTimeMs(data.server_time_ms)
        setLatencyMs(Date.now() - start)
        setLoading(false)
      } catch {
        // Silently retry on next interval
        if (mountedRef.current) setLoading(false)
      }
    }

    fetchViews()
    const timer = setInterval(fetchViews, intervalMs)

    return () => {
      mountedRef.current = false
      clearInterval(timer)
    }
  }, [baseUrl, intervalMs])

  return { views, serverTimeMs, latencyMs, loading }
}
