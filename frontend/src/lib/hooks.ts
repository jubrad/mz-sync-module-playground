import { useCallback, useEffect, useMemo, useRef, useState, useSyncExternalStore } from 'react'
import { useMzContext } from './context'
import type { MzTimestamp, QueryOpts, QueryResult, ViewInfo, ViewsResponse } from './types'

// ── useMzQuery — filtered/sorted query subscription ──────────────

export function useMzQuery<T = Record<string, unknown>>(
  viewName: string | null,
  opts: QueryOpts<T> = {},
): QueryResult<T> & { ready: boolean; connected: boolean } {
  const { store } = useMzContext()

  // Keep opts in a ref so the subscription stays stable
  const optsRef = useRef(opts)
  optsRef.current = opts

  useEffect(() => {
    if (!viewName) return
    store.openView(viewName)
    return () => store.closeView(viewName)
  }, [store, viewName])

  const { subscribe, getSnapshot } = useMemo(() => {
    if (!viewName) {
      const empty: QueryResult<T> = { rows: [], totalCount: 0, filteredCount: 0 }
      return { subscribe: () => () => {}, getSnapshot: () => empty }
    }
    return store.createQuery<T>(viewName, optsRef)
  }, [store, viewName])

  const result = useSyncExternalStore(subscribe, getSnapshot)

  const view = viewName ? store._getView(viewName) : null
  return useMemo(() => ({
    ...result,
    ready: view?.ready ?? false,
    connected: view?.connected ?? false,
  }), [result, view?.ready, view?.connected])
}

// ── useMzRow — single row from a synced view ─────────────────────

export function useMzRow<T = Record<string, unknown>>(
  viewName: string | null,
  key: string | null,
): { value: T | undefined; ready: boolean } {
  const { store } = useMzContext()

  useEffect(() => {
    if (!viewName) return
    store.openView(viewName)
    return () => store.closeView(viewName)
  }, [store, viewName])

  const { subscribe, getSnapshot } = useMemo(() => {
    if (!viewName || !key) {
      return { subscribe: () => () => {}, getSnapshot: () => undefined as T | undefined }
    }
    return store.createRowSub<T>(viewName, key)
  }, [store, viewName, key])

  const value = useSyncExternalStore(subscribe, getSnapshot)
  const view = viewName ? store._getView(viewName) : null
  return { value, ready: view?.ready ?? false }
}

// ── useMzCount — count with optional filter ──────────────────────

export function useMzCount<T = Record<string, unknown>>(
  viewName: string | null,
  filter?: (row: T, key: string) => boolean,
): number {
  const { store } = useMzContext()
  const filterRef = useRef(filter)
  filterRef.current = filter

  useEffect(() => {
    if (!viewName) return
    store.openView(viewName)
    return () => store.closeView(viewName)
  }, [store, viewName])

  const { subscribe, getSnapshot } = useMemo(() => {
    if (!viewName) {
      return { subscribe: () => () => {}, getSnapshot: () => 0 }
    }
    return store.createCountSub<T>(viewName, filterRef)
  }, [store, viewName])

  return useSyncExternalStore(subscribe, getSnapshot)
}

// ── useMzViews — poll /api/views metadata ────────────────────────

export interface MzViewsMeta {
  views: ViewInfo[]
  serverTimeMs: number
  latencyMs: number
  loading: boolean
}

export function useMzViews(intervalMs = 3000): MzViewsMeta {
  const { baseUrl } = useMzContext()
  const [state, setState] = useState<MzViewsMeta>({
    views: [],
    serverTimeMs: 0,
    latencyMs: 0,
    loading: true,
  })

  useEffect(() => {
    let alive = true
    const poll = async () => {
      const t0 = performance.now()
      try {
        const r = await fetch(`${baseUrl}/api/views`)
        const body: ViewsResponse = await r.json()
        if (!alive) return
        setState({
          views: body.views || [],
          serverTimeMs: body.server_time_ms || 0,
          latencyMs: performance.now() - t0,
          loading: false,
        })
      } catch {
        if (!alive) return
        setState((s) => ({ ...s, loading: false }))
      }
    }
    poll()
    const id = setInterval(poll, intervalMs)
    return () => { alive = false; clearInterval(id) }
  }, [baseUrl, intervalMs])

  return state
}

// ── useMzKey — single key REST lookup (not streaming) ────────────

export function useMzKey<T = unknown>(viewName: string | null, key: string | null) {
  const { baseUrl } = useMzContext()
  const [value, setValue] = useState<T | null>(null)
  const [loading, setLoading] = useState(false)

  const refetch = useCallback(async () => {
    if (!viewName || !key) { setValue(null); return }
    setLoading(true)
    try {
      const r = await fetch(`${baseUrl}/api/views/${encodeURIComponent(viewName)}/${encodeURIComponent(key)}`)
      if (r.ok) setValue(await r.json())
      else setValue(null)
    } catch { setValue(null) }
    setLoading(false)
  }, [baseUrl, viewName, key])

  useEffect(() => { refetch() }, [refetch])

  return { value, loading, refetch }
}
