import { useState, useEffect, useRef } from 'react'
import { useMzContext } from './context.js'

export interface UseFreshnessResult {
  /** Rolling 3-second average staleness in milliseconds. */
  avgMs: number
  /** Freshness color: green (<500ms), yellow (<2000ms), red (>=2000ms). */
  color: 'green' | 'yellow' | 'red'
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
  const [avgMs, setAvgMs] = useState(0)
  const samplesRef = useRef<number[]>([])

  useEffect(() => {
    if (!sqliteSink) return

    // Rolling window: 3 seconds / 200ms = 15 samples
    const MAX_SAMPLES = 15

    const timer = setInterval(() => {
      const lastCommit = sqliteSink.lastCommitTime
      const staleness = lastCommit > 0 ? Date.now() - lastCommit : 0
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
