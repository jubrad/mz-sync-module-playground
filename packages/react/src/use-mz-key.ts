import { useState, useEffect, useCallback, useRef } from 'react'
import { useMzContext } from './context.js'

export interface UseMzKeyResult<T> {
  value: T | undefined
  loading: boolean
  refetch: () => void
}

/**
 * Fetch a single key from a Materialize view via the REST API.
 *
 * @param viewName - The view to query
 * @param key      - The key to look up
 */
export function useMzKey<T = Record<string, unknown>>(
  viewName: string,
  key: string,
): UseMzKeyResult<T> {
  const { baseUrl } = useMzContext()
  const [value, setValue] = useState<T | undefined>(undefined)
  const [loading, setLoading] = useState(true)
  const mountedRef = useRef(true)

  const fetchKey = useCallback(async () => {
    setLoading(true)
    try {
      const res = await fetch(
        `${baseUrl}/api/views/${encodeURIComponent(viewName)}/${encodeURIComponent(key)}`,
      )
      if (!res.ok) {
        if (mountedRef.current) setValue(undefined)
        return
      }
      const data = await res.json()
      if (!mountedRef.current) return
      setValue(data as T)
    } catch {
      if (mountedRef.current) setValue(undefined)
    } finally {
      if (mountedRef.current) setLoading(false)
    }
  }, [baseUrl, viewName, key])

  useEffect(() => {
    mountedRef.current = true
    fetchKey()
    return () => {
      mountedRef.current = false
    }
  }, [fetchKey])

  return { value, loading, refetch: fetchKey }
}
