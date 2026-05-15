import { useState, useEffect, useRef } from 'react'
import { useMzContext } from './context.js'

export interface UseFreshnessResult {
  /** Rolling 3-second average staleness in milliseconds. -1 means no data received yet. */
  avgMs: number
  /** Freshness color: gray (no data), green (<500ms), yellow (<2000ms), red (>=2000ms). */
  color: 'gray' | 'green' | 'yellow' | 'red'
  /** Human-readable label for the current freshness. */
  label: string
}

/**
 * Measures how fresh the local SQLite sink data is by sampling
 * `lastCommitTime` every 200ms and computing a rolling 3-second
 * average staleness.
 */
export function useFreshness(): UseFreshnessResult {
  const { sqliteSink } = useMzContext()
  const [avgMs, setAvgMs] = useState(-1)
  const samplesRef = useRef<number[]>([])
  const hasReceivedDataRef = useRef(false)

  useEffect(() => {
    if (!sqliteSink) return

    hasReceivedDataRef.current = false

    // Rolling window: 3 seconds / 200ms = 15 samples
    const MAX_SAMPLES = 15

    const timer = setInterval(() => {
      const lastCommit = sqliteSink.lastCommitTime
      if (lastCommit === 0) {
        // No data received yet
        setAvgMs(-1)
        return
      }

      hasReceivedDataRef.current = true
      const staleness = Date.now() - lastCommit
      const samples = samplesRef.current

      samples.push(staleness)
      if (samples.length > MAX_SAMPLES) {
        samples.shift()
      }

      const avg =
        samples.length > 0
          ? samples.reduce((a, b) => a + b, 0) / samples.length
          : 0

      setAvgMs(Math.round(avg))
    }, 200)

    return () => {
      clearInterval(timer)
      samplesRef.current = []
    }
  }, [sqliteSink])

  if (avgMs < 0) {
    return { avgMs: -1, color: 'gray', label: 'Waiting' }
  }

  const color: UseFreshnessResult['color'] =
    avgMs < 500 ? 'green' : avgMs < 2000 ? 'yellow' : 'red'

  const label =
    color === 'green'
      ? 'Fresh'
      : color === 'yellow'
        ? 'Stale'
        : 'Very stale'

  return { avgMs, color, label }
}
