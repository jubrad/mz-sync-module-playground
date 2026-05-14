/**
 * MzStore — reactive in-memory store for Materialize views.
 *
 * - One store per app, manages all views with ref-counted transports
 * - Batches mutations between progress boundaries (1 commit = 1 notify)
 * - Row-level change tracking + structural sharing
 * - Query subscriptions with memoized results for useSyncExternalStore
 * - localStorage persistence
 */
import { MzTransport } from './mz-sync'
import type { ChangeEvent, MzTimestamp, MzStoreOptions, QueryOpts, QueryResult, Transport } from './types'

// ── Row wrapper cache ──────────────────────────────────────────

/** Caches { _key, ...value } wrappers so unchanged rows keep reference identity. */
class RowCache<T> {
  private cache = new Map<string, T & { _key: string }>()
  private refs = new Map<string, T>()

  get(key: string, value: T): T & { _key: string } {
    if (this.refs.get(key) === value) return this.cache.get(key)!
    const wrapped = { _key: key, ...(typeof value === 'object' && value !== null ? value : { _value: value }) } as T & { _key: string }
    this.cache.set(key, wrapped)
    this.refs.set(key, value)
    return wrapped
  }

  delete(key: string) {
    this.cache.delete(key)
    this.refs.delete(key)
  }
}

// ── View state ─────────────────────────────────────────────────

class ViewState {
  data = new Map<string, any>()

  // Batch buffer
  pendingUpserts = new Map<string, any>()
  pendingDeletes = new Set<string>()

  progress: MzTimestamp = 0
  initialProgress: MzTimestamp = 0  // progress loaded from localStorage (pre-snapshot)
  ready = false
  connected = false
  version = 0
  lastChangedKeys = new Set<string>()

  transport: MzTransport | null = null
  refCount = 0

  // View-level subscribers (notified on commit)
  listeners = new Set<() => void>()
}

// ── Query subscription ─────────────────────────────────────────

class QuerySub<T = any> {
  private store: MzStore
  private viewName: string
  private optsRef: { current: QueryOpts<T> }
  private rowCache = new RowCache<T>()
  private lastVersion = -1
  private lastResult: QueryResult<T> = { rows: [], totalCount: 0, filteredCount: 0 }
  listeners = new Set<() => void>()

  constructor(store: MzStore, viewName: string, optsRef: { current: QueryOpts<T> }) {
    this.store = store
    this.viewName = viewName
    this.optsRef = optsRef
  }

  getSnapshot = (): QueryResult<T> => {
    const view = this.store._getView(this.viewName)
    if (!view || view.version === this.lastVersion) return this.lastResult
    this.lastVersion = view.version

    const { filter, sort, limit, offset = 0 } = this.optsRef.current

    let filtered: (T & { _key: string })[] = []
    for (const [key, value] of view.data) {
      if (filter && !filter(value, key)) continue
      filtered.push(this.rowCache.get(key, value))
    }
    const filteredCount = filtered.length

    if (sort) filtered.sort(sort)

    if (offset > 0 || limit !== undefined) {
      filtered = filtered.slice(offset, limit !== undefined ? offset + limit : undefined)
    }

    // Structural sharing: if result array is identical by reference, keep old object
    if (arrEq(filtered, this.lastResult.rows) && filteredCount === this.lastResult.filteredCount) {
      return this.lastResult
    }

    this.lastResult = { rows: filtered, totalCount: view.data.size, filteredCount }
    return this.lastResult
  }

  subscribe = (cb: () => void): (() => void) => {
    this.listeners.add(cb)
    // Also subscribe to view-level changes
    const view = this.store._getView(this.viewName)
    if (view) view.listeners.add(cb)
    return () => {
      this.listeners.delete(cb)
      if (view) view.listeners.delete(cb)
    }
  }

  prune(deletedKeys: Set<string>) {
    for (const key of deletedKeys) this.rowCache.delete(key)
  }
}

// ── Row subscription ───────────────────────────────────────────

class RowSub<T = any> {
  private store: MzStore
  private viewName: string
  private key: string
  private lastVersion = -1
  private lastValue: T | undefined = undefined

  constructor(store: MzStore, viewName: string, key: string) {
    this.store = store
    this.viewName = viewName
    this.key = key
  }

  getSnapshot = (): T | undefined => {
    const view = this.store._getView(this.viewName)
    if (!view) return undefined
    if (view.version === this.lastVersion) return this.lastValue
    this.lastVersion = view.version
    this.lastValue = view.data.get(this.key)
    return this.lastValue
  }

  subscribe = (cb: () => void): (() => void) => {
    const view = this.store._getView(this.viewName)
    if (view) view.listeners.add(cb)
    return () => { if (view) view.listeners.delete(cb) }
  }
}

// ── Count subscription ─────────────────────────────────────────

class CountSub<T = any> {
  private store: MzStore
  private viewName: string
  private filterRef: { current?: (row: T, key: string) => boolean }
  private lastVersion = -1
  private lastCount = 0

  constructor(store: MzStore, viewName: string, filterRef: { current?: (row: T, key: string) => boolean }) {
    this.store = store
    this.viewName = viewName
    this.filterRef = filterRef
  }

  getSnapshot = (): number => {
    const view = this.store._getView(this.viewName)
    if (!view) return 0
    if (view.version === this.lastVersion) return this.lastCount
    this.lastVersion = view.version
    const filter = this.filterRef.current
    if (!filter) { this.lastCount = view.data.size; return this.lastCount }
    let count = 0
    for (const [key, value] of view.data) {
      if (filter(value, key)) count++
    }
    this.lastCount = count
    return this.lastCount
  }

  subscribe = (cb: () => void): (() => void) => {
    const view = this.store._getView(this.viewName)
    if (view) view.listeners.add(cb)
    return () => { if (view) view.listeners.delete(cb) }
  }
}

// ── MzStore ────────────────────────────────────────────────────

export class MzStore {
  private baseUrl: string
  private transport: Transport
  private reconnectMs: number
  private persist: boolean
  private views = new Map<string, ViewState>()
  private querySubs = new Set<QuerySub>()
  private saveTimers = new Map<string, ReturnType<typeof setTimeout>>()
  private flushId: number | null = null

  constructor(baseUrl: string, opts?: MzStoreOptions) {
    this.baseUrl = baseUrl.replace(/\/$/, '')
    this.transport = opts?.transport || 'ws'
    this.reconnectMs = opts?.reconnectMs ?? 2000
    this.persist = opts?.persist ?? true
  }

  /** Open a view (ref-counted). Creates transport on first open. */
  openView(viewName: string) {
    let view = this.views.get(viewName)
    if (!view) {
      view = new ViewState()
      this.views.set(viewName, view)
      // Load localStorage for instant render, but don't use its progress
      // for the initial connection — always start with a fresh snapshot.
      // Progress is only used for reconnections within the same session.
      if (this.persist) this.loadFromStorage(viewName, view)
      view.initialProgress = view.progress
      view.progress = 0  // force snapshot on first connect
    }
    view.refCount++
    if (view.refCount === 1 && !view.transport) {
      const t = new MzTransport(this.baseUrl, viewName, {
        onEvent: (ev) => this.handleEvent(viewName, ev),
        onConnected: () => { view!.connected = true; this.notifyView(viewName) },
        onDisconnected: () => { view!.connected = false; this.notifyView(viewName) },
        getProgress: () => view!.progress,
      }, { transport: this.transport, reconnectMs: this.reconnectMs })
      view.transport = t
      t.connect()
    }
  }

  /** Close a view (ref-counted). Tears down transport when refcount hits 0. */
  closeView(viewName: string) {
    const view = this.views.get(viewName)
    if (!view) return
    view.refCount--
    if (view.refCount <= 0) {
      view.transport?.disconnect()
      view.transport = null
      view.refCount = 0
    }
  }

  /** Force a full resync — clears all local data and reconnects with snapshot. */
  resync() {
    for (const [name, view] of this.views) {
      view.data.clear()
      view.pendingUpserts.clear()
      view.pendingDeletes.clear()
      view.progress = 0
      view.version++
      view.ready = false
      this.notifyView(name)
      if (view.transport) {
        view.transport.disconnect()
        view.transport.connect()
      }
      if (this.persist) {
        try { localStorage.removeItem(this.storageKey(name)) } catch {}
      }
    }
  }

  /** Expose view state for hooks (internal). */
  _getView(viewName: string): ViewState | undefined {
    return this.views.get(viewName)
  }

  /** Create a query subscription compatible with useSyncExternalStore. */
  createQuery<T>(viewName: string, optsRef: { current: QueryOpts<T> }): {
    subscribe: (cb: () => void) => () => void
    getSnapshot: () => QueryResult<T>
  } {
    const sub = new QuerySub<T>(this, viewName, optsRef)
    this.querySubs.add(sub)
    return { subscribe: sub.subscribe, getSnapshot: sub.getSnapshot }
  }

  /** Single-row subscription. */
  createRowSub<T>(viewName: string, key: string): {
    subscribe: (cb: () => void) => () => void
    getSnapshot: () => T | undefined
  } {
    const sub = new RowSub<T>(this, viewName, key)
    return { subscribe: sub.subscribe, getSnapshot: sub.getSnapshot }
  }

  /** Count subscription. */
  createCountSub<T>(viewName: string, filterRef: { current?: (row: T, key: string) => boolean }): {
    subscribe: (cb: () => void) => () => void
    getSnapshot: () => number
  } {
    const sub = new CountSub<T>(this, viewName, filterRef)
    return { subscribe: sub.subscribe, getSnapshot: sub.getSnapshot }
  }

  destroy() {
    if (this.flushId !== null) { cancelAnimationFrame(this.flushId); this.flushId = null }
    for (const [, view] of this.views) {
      view.transport?.disconnect()
    }
    this.views.clear()
    for (const [, timer] of this.saveTimers) clearTimeout(timer)
    this.saveTimers.clear()
  }

  /** Flush pending changes on the next animation frame for smooth rendering. */
  private scheduleFlush() {
    if (this.flushId !== null) return
    this.flushId = requestAnimationFrame(() => {
      this.flushId = null
      for (const [name, view] of this.views) {
        if (view.pendingUpserts.size > 0 || view.pendingDeletes.size > 0) {
          this.commitBatch(name, view.progress)
        }
      }
    })
  }

  // ── Event handling ──────────────────────────────────

  private handleEvent(viewName: string, event: ChangeEvent) {
    const view = this.views.get(viewName)
    if (!view) return

    switch (event.type) {
      case 'upsert': {
        let val: any
        try {
          val = typeof event.value === 'string' ? JSON.parse(event.value) : event.value
        } catch {
          val = event.value
        }
        view.pendingUpserts.set(event.key!, val)
        view.pendingDeletes.delete(event.key!)
        this.scheduleFlush()
        break
      }
      case 'delete': {
        view.pendingDeletes.add(event.key!)
        view.pendingUpserts.delete(event.key!)
        this.scheduleFlush()
        break
      }
      case 'progress': {
        this.commitBatch(viewName, event.timestamp ?? 0)
        break
      }
      case 'snapshot_complete': {
        // Full snapshot received — clear any stale localStorage data.
        // The snapshot upserts already replaced everything, but delete
        // any keys that weren't in the snapshot (ghost rows from old sessions).
        const snapshotKeys = new Set(view.pendingUpserts.keys())
        for (const key of view.data.keys()) {
          if (!snapshotKeys.has(key)) {
            view.pendingDeletes.add(key)
          }
        }
        this.commitBatch(viewName, event.timestamp ?? view.progress)
        if (!view.ready) view.ready = true
        this.notifyView(viewName)
        break
      }
      case 'catchup_complete': {
        this.commitBatch(viewName, event.timestamp ?? view.progress)
        if (!view.ready) view.ready = true
        this.notifyView(viewName)
        break
      }
      case 'lagged': {
        // Reconnect
        view.transport?.disconnect()
        view.transport?.connect()
        break
      }
    }
  }

  private commitBatch(viewName: string, progressTs: MzTimestamp) {
    const view = this.views.get(viewName)
    if (!view) return

    if (view.pendingUpserts.size === 0 && view.pendingDeletes.size === 0) {
      view.progress = Math.max(view.progress, progressTs)
      return
    }

    const changed = new Set<string>()

    for (const [key, value] of view.pendingUpserts) {
      view.data.set(key, value)
      changed.add(key)
    }

    for (const key of view.pendingDeletes) {
      view.data.delete(key)
      changed.add(key)
    }

    view.pendingUpserts.clear()
    view.pendingDeletes.clear()
    view.progress = Math.max(view.progress, progressTs)
    view.version++
    view.lastChangedKeys = changed

    // Prune row caches for deleted keys
    for (const sub of this.querySubs) {
      sub.prune(changed)
    }

    this.notifyView(viewName)
    if (this.persist) this.scheduleSave(viewName)
  }

  private notifyView(viewName: string) {
    const view = this.views.get(viewName)
    if (!view) return
    for (const cb of view.listeners) {
      try { cb() } catch { /* ignore */ }
    }
  }

  // ── localStorage persistence ────────────────────────

  private storageKey(viewName: string) { return `mz:${viewName}` }

  private loadFromStorage(viewName: string, view: ViewState) {
    try {
      const raw = localStorage.getItem(this.storageKey(viewName))
      if (!raw) return
      const { data, progress } = JSON.parse(raw)
      if (data && typeof data === 'object') {
        for (const [k, v] of Object.entries(data)) {
          view.data.set(k, v)
        }
      }
      if (progress > 0) view.progress = progress
      if (view.data.size > 0) {
        view.ready = true
        view.version++
      }
    } catch { /* ignore corrupt storage */ }
  }

  private scheduleSave(viewName: string) {
    // Only the active/visible tab writes to localStorage to avoid cross-tab thrashing.
    if (typeof document !== 'undefined' && document.visibilityState === 'hidden') return
    const existing = this.saveTimers.get(viewName)
    if (existing) clearTimeout(existing)
    this.saveTimers.set(viewName, setTimeout(() => {
      if (typeof document !== 'undefined' && document.visibilityState === 'hidden') return
      const view = this.views.get(viewName)
      if (!view) return
      try {
        localStorage.setItem(this.storageKey(viewName), JSON.stringify({
          data: Object.fromEntries(view.data),
          progress: view.progress,
        }))
      } catch { /* storage full */ }
    }, 500))
  }
}

// ── Utilities ──────────────────────────────────────────────────

function arrEq<T>(a: T[], b: T[]): boolean {
  if (a.length !== b.length) return false
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false
  }
  return true
}
