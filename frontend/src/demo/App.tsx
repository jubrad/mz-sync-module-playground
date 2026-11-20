import { useState, useMemo, useCallback, useEffect, useRef } from 'react'
import { MzSubscribeClient } from '@mz-web/subscribe'
import { MzHttpTransport } from '@mz-web/transport-http'
import { MzSqliteSink } from '@mz-web/sink-sqlite'
import { WasmSqlite } from '@mz-web/sqlite-wasm'
import { MzProvider, useMzContext, useMzViews, useLiveQuery, useLiveQueryScalar, useFreshness } from '@mz-web/react'
import { MzTable } from '../components/MzTable'
// @ts-ignore — Vite worker URL import
import WorkerURL from '@mz-web/sqlite-wasm/worker?worker&url'

// ── Simulator API ───────────────────────────────────────────────

const SIM_URL = import.meta.env.DEV ? '' : 'http://localhost:9000'

async function openStores(count: number) {
  try {
    const r = await fetch(`${SIM_URL}/sim/stores?count=${count}`, { method: 'POST' })
    if (!r.ok) return null
    return await r.json()
  } catch { return null }
}

async function toggleSale(multiplier = 2) {
  try {
    const r = await fetch(`${SIM_URL}/sim/sale?multiplier=${multiplier}`, { method: 'POST' })
    if (!r.ok) return null
    return await r.json()
  } catch { return null }
}

function useSimStatus(intervalMs = 2000) {
  const [s, set] = useState({ orders_per_second: 0, sale_active: false, rate_multiplier: 1 })
  useEffect(() => {
    let alive = true
    const poll = async () => {
      try {
        const r = await fetch(`${SIM_URL}/sim/status`)
        if (r.ok && alive) { const d = await r.json(); set({ orders_per_second: d.orders_per_second ?? 0, sale_active: d.sale_active ?? false, rate_multiplier: d.rate_multiplier ?? 1 }) }
      } catch {}
    }
    poll()
    const id = setInterval(poll, intervalMs)
    return () => { alive = false; clearInterval(id) }
  }, [intervalMs])
  return s
}

// ── Constants ───────────────────────────────────────────────────

const STATUSES = [
  { key: 'placed',            label: 'Placed',           color: '#4263eb', bg: '#dbe4ff' },
  { key: 'being_made',        label: 'Being Made',       color: '#f76707', bg: '#fff4e6' },
  { key: 'in_the_oven',       label: 'In Oven',          color: '#e03131', bg: '#ffe3e3' },
  { key: 'final_prep',        label: 'Final Prep',       color: '#ae3ec9', bg: '#f3d9fa' },
  { key: 'waiting_on_driver', label: 'Waiting',          color: '#e8590c', bg: '#fff4e6' },
  { key: 'on_its_way',        label: 'On Its Way',       color: '#2b8a3e', bg: '#d3f9d8' },
] as const

const ALL_VIEWS = ['store_dashboard', 'top_sellers', 'slow_orders_by_store', 'slow_orders', 'live_orders', 'recent_deliveries', 'driver_board']

function formatCents(c: number) { return `$${(c / 100).toFixed(2)}` }

// ── Dashboard ───────────────────────────────────────────────────

function Dashboard() {
  useMzViews(2000)
  const { client, sqliteSink } = useMzContext()
  const sim = useSimStatus(2000)
  const [viewsReady, setViewsReady] = useState(false)
  const [search, setSearch] = useState('')
  const [selectedStoreId, setSelectedStoreId] = useState<number | null>(null)

  // Sync all views — full local replica, always up to date
  useEffect(() => {
    for (const v of ALL_VIEWS) client.addView(v)
    setViewsReady(true)
    return () => { for (const v of ALL_VIEWS) client.removeView(v) }
  }, [client])

  // ── Sidebar state ──────────────────────────────────
  const searchRef = useRef<HTMLInputElement>(null)

  // All stores (for sidebar list)
  const { rows: allStores } = useLiveQuery<any>(
    'SELECT * FROM store_dashboard ORDER BY name',
    [],
    ['store_dashboard'],
  )

  // Fuzzy filter
  const filteredStores = useMemo(() => {
    if (!search.trim()) return allStores
    const q = search.toLowerCase()
    return allStores.filter((s: any) =>
      (s.name ?? '').toLowerCase().includes(q) ||
      (s.city ?? '').toLowerCase().includes(q) ||
      (s.state ?? '').toLowerCase().includes(q)
    )
  }, [allStores, search])

  // Keyboard navigation
  // Arrows work from the search box or the list; Tab/Shift+Tab only from the list
  // so Tab out of the search box still moves focus normally.
  const listRef = useRef<HTMLDivElement>(null)
  const handleSidebarKeyDown = useCallback((e: React.KeyboardEvent) => {
    const tabInList = e.key === 'Tab' && e.target === listRef.current
    if (e.key === 'ArrowDown' || e.key === 'ArrowUp' || tabInList) {
      e.preventDefault()
      const forward = e.key === 'ArrowDown' || (tabInList && !e.shiftKey)
      const idx = filteredStores.findIndex((s: any) => s.id === selectedStoreId)
      const next = forward
        ? Math.min(idx + 1, filteredStores.length - 1)
        : Math.max(idx - 1, -1)
      setSelectedStoreId(next >= 0 ? filteredStores[next].id : null)
    } else if (e.key === 'Escape') {
      setSelectedStoreId(null)
      setSearch('')
    }
  }, [filteredStores, selectedStoreId])

  useEffect(() => {
    listRef.current?.querySelector('[data-selected="true"]')?.scrollIntoView({ block: 'nearest' })
  }, [selectedStoreId])

  // ── Header controls ────────────────────────────────
  const [opening, setOpening] = useState(false)
  const [showAddInput, setShowAddInput] = useState(false)
  const [addCount, setAddCount] = useState('')
  const [showDiag, setShowDiag] = useState(false)

  const handleOpen = useCallback(async () => {
    const n = Math.max(1, parseInt(addCount) || 1)
    setOpening(true)
    await openStores(n)
    setOpening(false); setAddCount(''); setShowAddInput(false)
  }, [addCount])

  const handleSale = useCallback(async (mult: number) => { await toggleSale(mult) }, [])

  // ── Store detail queries (only run when a store is selected) ──
  const hasStore = selectedStoreId != null
  const storeClause = hasStore ? ` WHERE "store_id" = ${selectedStoreId}` : ''
  const storeClauseDriver = hasStore
    ? ` WHERE "store_name" = (SELECT "name" FROM store_dashboard WHERE "id" = ${selectedStoreId})`
    : ''

  const { rows: pipelineRows } = useLiveQuery<{ status: string; count: number }>(
    hasStore ? `SELECT "status", count(*) as count FROM live_orders${storeClause} GROUP BY "status"` : null,
    [], ['live_orders'],
  )
  const { rows: orders } = useLiveQuery<any>(
    hasStore ? `SELECT * FROM live_orders${storeClause}` : null,
    [], ['live_orders'],
  )
  const { rows: deliveries } = useLiveQuery<any>(
    hasStore ? `SELECT * FROM recent_deliveries${storeClause}` : null,
    [], ['recent_deliveries'],
  )
  const { rows: drivers } = useLiveQuery<any>(
    hasStore ? `SELECT * FROM driver_board${storeClauseDriver}` : null,
    [], ['driver_board'],
  )
  const revenue = useLiveQueryScalar<number>(
    hasStore ? `SELECT coalesce(sum(total_cents),0) FROM live_orders${storeClause}` : null,
    [], ['live_orders'],
  ) ?? 0

  // ── Overview stats (only when no store selected) ────
  const overviewActive = useLiveQueryScalar<number>(
    !hasStore ? `SELECT count(*) FROM live_orders` : null,
    [], ['live_orders'],
  ) ?? 0
  const overviewDelivered = useLiveQueryScalar<number>(
    !hasStore ? `SELECT count(*) FROM recent_deliveries` : null,
    [], ['recent_deliveries'],
  ) ?? 0
  const overviewRevenue = useLiveQueryScalar<number>(
    !hasStore ? `SELECT coalesce(sum(total_cents),0) FROM live_orders` : null,
    [], ['live_orders'],
  ) ?? 0

  // Orders/sec — computed locally from created_at timestamps, over the 5
  // seconds before the newest order we hold. Measuring back from the data
  // instead of the wall clock keeps the rate meaningful while the stream lags
  // (the Freshness indicator shows the lag itself).
  const recentWindow = `"created_at" > datetime((SELECT substr(max("created_at"), 1, 19) FROM live_orders), '-5 seconds')`
  const ordersPerSec = useLiveQueryScalar<number>(
    hasStore
      ? `SELECT count(*) FROM live_orders WHERE "store_id" = ${selectedStoreId} AND ${recentWindow}`
      : `SELECT count(*) FROM live_orders WHERE ${recentWindow}`,
    [], ['live_orders'],
  )
  const ordersPerSecDisplay = ((ordersPerSec ?? 0) / 5).toFixed(1)

  const pipeline = useMemo(() => {
    const m: Record<string, number> = {}
    for (const r of pipelineRows) m[r.status] = Number(r.count)
    return STATUSES.map((s) => ({ ...s, count: m[s.key] ?? 0 }))
  }, [pipelineRows])
  const totalActive = hasStore ? pipeline.reduce((s, p) => s + p.count, 0) : overviewActive

  const orderCols = useMemo(() => {
    if (orders.length === 0) return []
    return Object.keys(orders[0]).filter((k) => !['store_id'].includes(k))
  }, [orders])

  const selectedStore = allStores.find((s: any) => s.id === selectedStoreId)

  if (!viewsReady) {
    return <div style={{ padding: 40, textAlign: 'center', color: '#868e96', fontFamily: 'system-ui' }}>Syncing views...</div>
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100vh', fontFamily: '-apple-system, BlinkMacSystemFont, system-ui, sans-serif' }}>
      {/* ── Header ─────────────────────────────────────── */}
      <header style={{
        background: 'linear-gradient(135deg, #c92a2a 0%, #e03131 100%)',
        color: '#fff', padding: '12px 24px', display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        flexShrink: 0,
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
          <span style={{ fontSize: 24 }}>🍕</span>
          <div>
            <div style={{ fontSize: 17, fontWeight: 700, letterSpacing: '-0.5px' }}>Fresh Delivery</div>
            <div style={{ fontSize: 11, opacity: 0.8 }}>
              {selectedStore ? `${selectedStore.name} — ${selectedStore.city}, ${selectedStore.state}` : 'Delivery Operations'}
            </div>
          </div>
        </div>
        <div style={{ display: 'flex', gap: 24, fontSize: 13, alignItems: 'center' }}>
          <Stat label="Active" value={totalActive} />
          <Stat label="Delivered" value={hasStore ? deliveries.length : overviewDelivered} />
          <Stat label="Revenue" value={formatCents(hasStore ? revenue : overviewRevenue)} />
          <Stat label="/sec" value={ordersPerSecDisplay} />
          {showAddInput ? (
            <div style={{
              display: 'flex', alignItems: 'center',
              background: 'rgba(255,255,255,0.15)', borderRadius: 8,
              border: '1px solid rgba(255,255,255,0.3)', overflow: 'hidden',
            }}>
              <input
                value={addCount}
                onChange={(e) => setAddCount(e.target.value.replace(/\D/g, ''))}
                onKeyDown={(e) => { if (e.key === 'Enter') handleOpen(); if (e.key === 'Escape') { setShowAddInput(false); setAddCount('') } }}
                placeholder="how many?"
                autoFocus
                style={{
                  width: 100, padding: '6px 12px', border: 'none', background: 'transparent',
                  color: '#fff', fontSize: 13, outline: 'none',
                }}
              />
              <button
                onClick={handleOpen}
                disabled={opening}
                style={{
                  background: 'rgba(255,255,255,0.2)', border: 'none', borderLeft: '1px solid rgba(255,255,255,0.2)',
                  color: '#fff', padding: '6px 14px', fontSize: 12, fontWeight: 700,
                  cursor: opening ? 'wait' : 'pointer',
                }}
              >
                {opening ? '...' : 'Add'}
              </button>
            </div>
          ) : (
            <button onClick={() => setShowAddInput(true)} disabled={opening} style={headerBtn}>
              {opening ? 'Adding...' : 'Add Stores'}
            </button>
          )}
          {sim.sale_active ? (
            <button onClick={() => handleSale(1)} style={{ ...headerBtn, background: sim.rate_multiplier >= 10 ? '#ff6b6b' : '#ffd43b', color: sim.rate_multiplier >= 10 ? '#fff' : '#e67700', border: `1px solid ${sim.rate_multiplier >= 10 ? '#e03131' : '#fab005'}` }}>
              End Sale ({sim.rate_multiplier}x)
            </button>
          ) : (
            <>
              <button onClick={() => handleSale(2)} style={headerBtn}>Sale 2x</button>
              <button onClick={() => handleSale(10)} style={{ ...headerBtn, background: 'rgba(255,100,100,0.3)', border: '1px solid rgba(255,100,100,0.5)' }}>10x</button>
            </>
          )}
          <Freshness />
          <button onClick={() => client.resync()} style={{ ...headerBtn, fontSize: 10, padding: '3px 8px', opacity: 0.7 }}>resync</button>
          <button onClick={() => setShowDiag((v) => !v)} style={{ ...headerBtn, fontSize: 10, padding: '3px 8px', opacity: 0.7 }}>diag</button>
        </div>
      </header>

      {showDiag && <DiagnosticsPanel onClose={() => setShowDiag(false)} />}

      {/* ── Body: sidebar + main ───────────────────────── */}
      <div style={{ display: 'flex', flex: 1, overflow: 'hidden' }}>
        {/* Sidebar */}
        <div onKeyDown={handleSidebarKeyDown} style={{ width: 280, flexShrink: 0, background: '#fff', borderRight: '1px solid #dee2e6', display: 'flex', flexDirection: 'column' }}>
          <div style={{ padding: '12px 12px 8px' }}>
            <input
              ref={searchRef}
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="Search stores..."
              style={{
                width: '100%', padding: '8px 12px', border: '1px solid #dee2e6', borderRadius: 8,
                fontSize: 13, background: '#f8f9fa', outline: 'none', boxSizing: 'border-box',
              }}
              onFocus={(e) => (e.target.style.borderColor = '#4263eb')}
              onBlur={(e) => (e.target.style.borderColor = '#dee2e6')}
            />
          </div>
          {/* All stores button */}
          <div
            onClick={() => setSelectedStoreId(null)}
            style={{
              padding: '10px 16px', cursor: 'pointer', fontSize: 13, fontWeight: 600,
              background: selectedStoreId == null ? '#dbe4ff' : '#fff',
              color: selectedStoreId == null ? '#4263eb' : '#495057',
              borderBottom: '1px solid #f1f3f5',
            }}
          >
            All Stores ({allStores.length})
          </div>
          {/* Store list */}
          <div ref={listRef} tabIndex={0} style={{ flex: 1, overflow: 'auto', outline: 'none' }}>
            {filteredStores.map((s: any) => {
              const active = Number(s.active_orders) || 0
              const isSelected = s.id === selectedStoreId
              return (
                <div
                  key={s.id}
                  data-selected={isSelected}
                  onClick={() => setSelectedStoreId(s.id)}
                  style={{
                    padding: '8px 16px', cursor: 'pointer',
                    background: isSelected ? '#e7f5ff' : '#fff',
                    borderBottom: '1px solid #f8f9fa',
                    borderLeft: isSelected ? '3px solid #4263eb' : '3px solid transparent',
                  }}
                >
                  <div style={{ fontSize: 13, fontWeight: 500, color: '#212529' }}>
                    {(s.name as string)}
                  </div>
                  <div style={{ fontSize: 11, color: '#868e96', display: 'flex', justifyContent: 'space-between' }}>
                    <span>{s.city}, {s.state}</span>
                    <span style={{ fontWeight: 600, color: active > 0 ? '#2b8a3e' : '#adb5bd' }}>{active}</span>
                  </div>
                </div>
              )
            })}
            {filteredStores.length === 0 && (
              <div style={{ padding: 20, textAlign: 'center', color: '#868e96', fontSize: 13 }}>No stores match</div>
            )}
          </div>
        </div>

        {/* Main content */}
        <div style={{ flex: 1, overflow: 'auto', background: '#f1f3f5', padding: 20 }}>
          {selectedStoreId == null ? (
            /* ── All Stores overview ──────────────────────── */
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 16 }}>
              <TopSellers storeClause="" />
              <TopStoresPanel onSelect={setSelectedStoreId} />
              <SlowestStoresPanel onSelect={setSelectedStoreId} />
            </div>
          ) : (
            /* ── Single store detail ─────────────────────── */
            <>
              {/* Pipeline */}
              <div style={{ display: 'flex', gap: 8, marginBottom: 16 }}>
                {pipeline.map((p) => (
                  <div key={p.key} style={{
                    flex: Math.max(p.count, 1), minWidth: 80,
                    background: p.bg, borderRadius: 8, padding: '10px 12px',
                    borderLeft: `3px solid ${p.color}`, transition: 'flex 0.4s ease',
                  }}>
                    <div style={{ fontSize: 22, fontWeight: 700, color: p.color, lineHeight: 1 }}>{p.count}</div>
                    <div style={{ fontSize: 10, fontWeight: 600, color: p.color, marginTop: 3, opacity: 0.8 }}>{p.label}</div>
                  </div>
                ))}
              </div>
              <div style={{ background: '#fff', borderRadius: 10, padding: 16, marginBottom: 16, boxShadow: '0 1px 3px rgba(0,0,0,0.06)' }}>
                <div style={{ fontSize: 12, fontWeight: 700, textTransform: 'uppercase', letterSpacing: 1, color: '#495057', marginBottom: 10 }}>
                  Live Orders ({orders.length})
                </div>
                <MzTable
                  rows={orders}
                  columns={orderCols}
                  onRowClick={() => {}}
                />
              </div>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 16 }}>
                <TopSellers storeClause={storeClause} />
                <DriversPanel drivers={drivers} storeId={selectedStoreId} />
                <SlowOrders storeClause={storeClause} />
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  )
}

// ── Bottom panels ───────────────────────────────────────────────

function TopSellers({ storeClause }: { storeClause: string }) {
  // When viewing all stores, read from the pre-computed MZ view (fast).
  // When viewing a single store, fall back to querying live_orders.
  const isGlobal = !storeClause
  const { rows } = useLiveQuery<{ items: string; cnt: number }>(
    isGlobal
      ? `SELECT "items", "cnt" FROM top_sellers ORDER BY "cnt" DESC LIMIT 8`
      : `SELECT "items", count(*) as cnt FROM live_orders${storeClause} GROUP BY "items" ORDER BY cnt DESC LIMIT 8`,
    [],
    isGlobal ? ['top_sellers'] : ['live_orders'],
  )
  return (
    <div style={{ background: '#fff', borderRadius: 10, padding: 16, boxShadow: '0 1px 3px rgba(0,0,0,0.06)' }}>
      <div style={{ fontSize: 12, fontWeight: 700, textTransform: 'uppercase', letterSpacing: 1, color: '#e67700', marginBottom: 10 }}>
        Top Sellers
      </div>
      <div style={{ maxHeight: 300, overflow: 'auto' }}>
        {rows.map((r, i) => {
          let label: string
          try { label = JSON.parse(r.items).join(', ') } catch { label = r.items }
          const pct = rows[0] ? (r.cnt / rows[0].cnt) * 100 : 0
          return (
            <div key={r.items} style={{ marginBottom: 6 }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: 12, marginBottom: 2 }}>
                <span style={{ fontWeight: i < 3 ? 700 : 400, color: '#212529', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: 220 }}>
                  {i < 3 ? ['🥇','🥈','🥉'][i] + ' ' : ''}{label}
                </span>
                <span style={{ fontWeight: 600, color: '#e67700', flexShrink: 0, marginLeft: 8 }}>{r.cnt}</span>
              </div>
              <div style={{ height: 3, background: '#f1f3f5', borderRadius: 2, overflow: 'hidden' }}>
                <div style={{ height: '100%', width: `${pct}%`, background: i < 3 ? '#e67700' : '#ffd8a8', borderRadius: 2 }} />
              </div>
            </div>
          )
        })}
        {rows.length === 0 && <div style={{ color: '#868e96', fontSize: 13, padding: 12, textAlign: 'center' }}>No orders yet</div>}
      </div>
    </div>
  )
}

function TopStoresPanel({ onSelect }: { onSelect: (id: number) => void }) {
  const { rows } = useLiveQuery<any>(
    `SELECT * FROM store_dashboard ORDER BY active_orders DESC LIMIT 10`,
    [], ['store_dashboard'],
  )
  return (
    <div style={{ background: '#fff', borderRadius: 10, padding: 16, boxShadow: '0 1px 3px rgba(0,0,0,0.06)' }}>
      <div style={{ fontSize: 12, fontWeight: 700, textTransform: 'uppercase', letterSpacing: 1, color: '#4263eb', marginBottom: 10 }}>
        Busiest Stores
      </div>
      <div style={{ maxHeight: 400, overflow: 'auto' }}>
        {rows.map((s: any, i: number) => {
          const active = Number(s.active_orders) || 0
          return (
            <div key={s.id} onClick={() => onSelect(s.id)} style={{ padding: '8px 0', borderBottom: '1px solid #f1f3f5', cursor: 'pointer', display: 'flex', justifyContent: 'space-between', alignItems: 'center', fontSize: 13 }}>
              <div>
                <span style={{ fontWeight: i < 3 ? 700 : 500 }}>{(s.name as string)}</span>
                <span style={{ fontSize: 11, color: '#868e96', marginLeft: 6 }}>{s.city}, {s.state}</span>
              </div>
              <span style={{
                fontWeight: 700, fontSize: 12, padding: '2px 8px', borderRadius: 4,
                background: active > 8 ? '#fff5f5' : active > 4 ? '#fff9db' : '#f8f9fa',
                color: active > 8 ? '#c92a2a' : active > 4 ? '#e67700' : '#495057',
              }}>{active}</span>
            </div>
          )
        })}
        {rows.length === 0 && <div style={{ color: '#868e96', fontSize: 13, padding: 12, textAlign: 'center' }}>No stores</div>}
      </div>
    </div>
  )
}

function SlowestStoresPanel({ onSelect }: { onSelect: (id: number) => void }) {
  // Read from MZ pre-computed view — no heavy SQL in SQLite
  const { rows } = useLiveQuery<any>(
    `SELECT * FROM slow_orders_by_store ORDER BY slow_count DESC LIMIT 10`,
    [], ['slow_orders_by_store'],
  )
  return (
    <div style={{
      background: rows.length > 0 ? '#fff5f5' : '#fff',
      borderRadius: 10, padding: 16, boxShadow: '0 1px 3px rgba(0,0,0,0.06)',
      border: rows.length > 0 ? '1px solid #ffc9c9' : '1px solid transparent',
    }}>
      <div style={{ fontSize: 12, fontWeight: 700, textTransform: 'uppercase', letterSpacing: 1, color: rows.length > 0 ? '#c92a2a' : '#868e96', marginBottom: 10 }}>
        Stores with Slow Orders
      </div>
      <div style={{ maxHeight: 400, overflow: 'auto' }}>
        {rows.map((s: any) => {
          const count = Number(s.slow_count) || 0
          const severity = count > 5 ? '#c92a2a' : count > 2 ? '#e67700' : '#f08c00'
          return (
            <div key={s.store_id} style={{ padding: '8px 0', borderBottom: '1px solid #ffc9c933', display: 'flex', justifyContent: 'space-between', alignItems: 'center', fontSize: 13 }}>
              <div onClick={() => onSelect(s.store_id)} style={{ cursor: 'pointer', flex: 1 }}>
                <span style={{ fontWeight: 500 }}>{(s.store_name as string)}</span>
                <span style={{ fontSize: 11, color: '#868e96', marginLeft: 6 }}>{s.store_city}, {s.store_state}</span>
              </div>
              <div style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
                <span style={{ fontWeight: 700, fontSize: 12, padding: '2px 10px', borderRadius: 4, background: '#ffe3e3', color: severity }}>
                  {count} slow
                </span>
                <button
                  onClick={(e) => { e.stopPropagation(); fetch(`${SIM_URL}/sim/drivers/${s.store_id}?count=5`, { method: 'POST' }) }}
                  style={{ background: '#2b8a3e', color: '#fff', border: 'none', borderRadius: 4, padding: '3px 8px', fontSize: 11, fontWeight: 600, cursor: 'pointer', whiteSpace: 'nowrap' }}
                >
                  +5 drivers
                </button>
              </div>
            </div>
          )
        })}
        {rows.length === 0 && <div style={{ color: '#2b8a3e', fontSize: 13, padding: 12, textAlign: 'center' }}>All stores on track</div>}
      </div>
    </div>
  )
}

function DriversPanel({ drivers, storeId }: { drivers: any[]; storeId: number | null }) {
  const available = drivers.filter((d: any) => d.status === 'available').length
  const delivering = drivers.length - available

  const adjustDrivers = useCallback(async (count: number) => {
    if (!storeId) return
    await fetch(`${SIM_URL}/sim/drivers/${storeId}?count=${count}`, { method: 'POST' })
  }, [storeId])

  return (
    <div style={{ background: '#fff', borderRadius: 10, padding: 16, boxShadow: '0 1px 3px rgba(0,0,0,0.06)' }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 10 }}>
        <div style={{ fontSize: 12, fontWeight: 700, textTransform: 'uppercase', letterSpacing: 1, color: '#495057' }}>
          Drivers ({drivers.length})
        </div>
        {storeId && (
          <div style={{ display: 'flex', gap: 4 }}>
            <button onClick={() => adjustDrivers(-1)} style={driverBtn} title="Fire 1 driver">-1</button>
            <button onClick={() => adjustDrivers(1)} style={driverBtn} title="Hire 1 driver">+1</button>
            <button onClick={() => adjustDrivers(5)} style={{ ...driverBtn, fontWeight: 700 }} title="Hire 5 drivers">+5</button>
          </div>
        )}
      </div>
      <div style={{ display: 'flex', gap: 12, marginBottom: 10, fontSize: 12 }}>
        <span style={{ color: '#2b8a3e', fontWeight: 600 }}>{available} available</span>
        <span style={{ color: '#e67700', fontWeight: 600 }}>{delivering} delivering</span>
      </div>
      <div style={{ maxHeight: 250, overflow: 'auto' }}>
        {drivers.map((d: any, i: number) => (
          <div key={`${d.id}-${i}`} style={{ padding: '5px 0', borderBottom: '1px solid #f1f3f5', display: 'flex', justifyContent: 'space-between', fontSize: 12 }}>
            <span>{d.name}</span>
            <span style={{
              fontWeight: 600, fontSize: 11, padding: '1px 6px', borderRadius: 4,
              background: d.status === 'available' ? '#d3f9d8' : '#fff4e6',
              color: d.status === 'available' ? '#2b8a3e' : '#e67700',
            }}>
              {d.status === 'delivering' ? `→ ${d.current_customer || '...'}` : 'idle'}
            </span>
          </div>
        ))}
        {drivers.length === 0 && <div style={{ color: '#868e96', fontSize: 13, padding: 12, textAlign: 'center' }}>No drivers</div>}
      </div>
    </div>
  )
}

const driverBtn: React.CSSProperties = {
  background: '#f8f9fa', border: '1px solid #dee2e6', borderRadius: 4,
  padding: '2px 8px', fontSize: 11, cursor: 'pointer', color: '#495057',
}

// Slow order threshold: 45 seconds from creation. Average lifecycle is ~38s,
// so this flags the ~25% slowest orders — enough to surface problems without noise.
const SLOW_THRESHOLD_SEC = 45

function SlowOrders({ storeClause }: { storeClause: string }) {
  // Read from MZ pre-computed slow_orders view
  const { rows } = useLiveQuery<any>(
    `SELECT * FROM slow_orders${storeClause} LIMIT 20`,
    [], ['slow_orders'],
  )

  return (
    <div style={{
      background: rows.length > 0 ? '#fff5f5' : '#fff',
      borderRadius: 10, padding: 16, boxShadow: '0 1px 3px rgba(0,0,0,0.06)',
      border: rows.length > 0 ? '1px solid #ffc9c9' : '1px solid transparent',
    }}>
      <div style={{ fontSize: 12, fontWeight: 700, textTransform: 'uppercase', letterSpacing: 1, color: rows.length > 0 ? '#c92a2a' : '#868e96', marginBottom: 10, display: 'flex', justifyContent: 'space-between' }}>
        <span>Slow Orders ({rows.length})</span>
        {rows.length > 0 && <span style={{ fontSize: 10, fontWeight: 400, color: '#e03131' }}>&gt; {SLOW_THRESHOLD_SEC}s</span>}
      </div>
      <div style={{ maxHeight: 300, overflow: 'auto' }}>
        {rows.map((r: any, i: number) => (
          <div key={`${r.id}-${i}`} style={{ padding: '6px 0', borderBottom: '1px solid #ffc9c933', display: 'flex', justifyContent: 'space-between', alignItems: 'center', fontSize: 13 }}>
            <div>
              <span style={{ fontWeight: 500 }}>{r.customer_name}</span>
              <span style={{ fontSize: 11, color: '#868e96', marginLeft: 6 }}>#{r.id}</span>
            </div>
            <span style={{ fontWeight: 600, fontSize: 11, padding: '2px 8px', borderRadius: 4, background: '#ffe3e3', color: '#c92a2a' }}>
              {r.status}
            </span>
          </div>
        ))}
        {rows.length === 0 && <div style={{ color: '#868e96', fontSize: 13, padding: 12, textAlign: 'center' }}>All orders on track</div>}
      </div>
    </div>
  )
}

// ── Small components ────────────────────────────────────────────

function DiagnosticsPanel({ onClose }: { onClose: () => void }) {
  const { client, sqliteSink } = useMzContext()
  const [diag, setDiag] = useState<any>(null)

  useEffect(() => {
    if (!sqliteSink) return
    let alive = true
    const poll = async () => {
      const d = await sqliteSink.getDiagnostics()
      // Merge connection status from subscribe client
      const views = client.getViews()
      const connMap = new Map(views.map((v) => [v.view, v.connected]))
      d.tables = d.tables.map((t: any) => ({ ...t, connected: connMap.get(t.name) ?? false }))
      if (alive) setDiag(d)
    }
    poll()
    const id = setInterval(poll, 1000)
    return () => { alive = false; clearInterval(id) }
  }, [client, sqliteSink])

  if (!diag) return null

  const formatBytes = (b: number) => {
    if (b < 1024) return `${b} B`
    if (b < 1024 * 1024) return `${(b / 1024).toFixed(1)} KB`
    return `${(b / (1024 * 1024)).toFixed(1)} MB`
  }

  return (
    <div style={{
      background: '#1a1a2e', color: '#e0e0e0', padding: '16px 24px',
      fontFamily: 'SF Mono, Consolas, monospace', fontSize: 12, lineHeight: 1.6,
      borderBottom: '2px solid #4263eb',
    }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
        <span style={{ fontSize: 13, fontWeight: 700, color: '#4263eb', textTransform: 'uppercase', letterSpacing: 1 }}>
          Diagnostics
        </span>
        <button onClick={onClose} style={{ background: 'none', border: 'none', color: '#868e96', cursor: 'pointer', fontSize: 14 }}>x</button>
      </div>

      {/* Summary stats */}
      <div style={{ display: 'flex', gap: 32, marginBottom: 14 }}>
        <div>
          <div style={{ color: '#868e96', fontSize: 10, textTransform: 'uppercase' }}>SQLite DB Size</div>
          <div style={{ fontSize: 16, fontWeight: 700, color: '#4263eb' }}>{formatBytes(diag.dbSizeBytes)}</div>
        </div>
        <div>
          <div style={{ color: '#868e96', fontSize: 10, textTransform: 'uppercase' }}>Total Writes</div>
          <div style={{ fontSize: 16, fontWeight: 700, color: '#e67700' }}>{diag.totalWrites.toLocaleString()}</div>
        </div>
        <div>
          <div style={{ color: '#868e96', fontSize: 10, textTransform: 'uppercase' }}>Total Commits</div>
          <div style={{ fontSize: 16, fontWeight: 700, color: '#2b8a3e' }}>{diag.totalCommits.toLocaleString()}</div>
        </div>
        <div>
          <div style={{ color: '#868e96', fontSize: 10, textTransform: 'uppercase' }}>Total Queries</div>
          <div style={{ fontSize: 16, fontWeight: 700, color: '#ae3ec9' }}>{diag.totalQueries.toLocaleString()}</div>
        </div>
        <div>
          <div style={{ color: '#868e96', fontSize: 10, textTransform: 'uppercase' }}>Version</div>
          <div style={{ fontSize: 16, fontWeight: 700, color: '#868e96' }}>{diag.version.toLocaleString()}</div>
        </div>
      </div>

      {/* Per-table breakdown */}
      <table style={{ width: '100%', borderCollapse: 'collapse' }}>
        <thead>
          <tr>
            {['Table', 'Rows', 'Transport', 'Status'].map((h) => (
              <th key={h} style={{ textAlign: 'left', padding: '4px 12px', color: '#868e96', fontSize: 10, textTransform: 'uppercase', borderBottom: '1px solid #333' }}>{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {diag.tables.map((t: any) => (
            <tr key={t.name}>
              <td style={{ padding: '4px 12px', color: '#e0e0e0', fontWeight: 600 }}>{t.name}</td>
              <td style={{ padding: '4px 12px', color: '#4263eb', fontWeight: 700 }}>{t.rows.toLocaleString()}</td>
              <td style={{ padding: '4px 12px', color: '#868e96' }}>WebSocket</td>
              <td style={{ padding: '4px 12px' }}>
                <span style={{
                  display: 'inline-block', width: 8, height: 8, borderRadius: '50%',
                  background: t.connected ? '#2b8a3e' : '#c92a2a', marginRight: 6,
                }} />
                {t.connected ? 'connected' : 'disconnected'}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function Freshness() {
  const { avgMs, color } = useFreshness()
  const colorMap: Record<string, string> = { green: '#69db7c', yellow: '#ffd43b', red: '#ff6b6b' }
  const label = avgMs < 1000 ? `${avgMs}ms` : `${(avgMs / 1000).toFixed(1)}s`

  return (
    <span style={{ fontSize: 11, opacity: 0.9, display: 'flex', alignItems: 'center', gap: 4 }}>
      <span style={{ display: 'inline-block', width: 6, height: 6, borderRadius: '50%', background: colorMap[color] ?? '#69db7c' }} />
      {label} unfresh
    </span>
  )
}

const headerBtn: React.CSSProperties = {
  background: 'rgba(255,255,255,0.2)', border: '1px solid rgba(255,255,255,0.3)',
  color: '#fff', borderRadius: 6, padding: '5px 12px', fontSize: 12,
  fontWeight: 600, cursor: 'pointer',
}

function Stat({ label, value }: { label: string; value: string | number }) {
  return (
    <div style={{ textAlign: 'center' }}>
      <div style={{ fontSize: 18, fontWeight: 700, lineHeight: 1 }}>{value}</div>
      <div style={{ fontSize: 10, opacity: 0.7, marginTop: 2 }}>{label}</div>
    </div>
  )
}

// ── Root ────────────────────────────────────────────────────────

export default function App() {
  const baseUrl = ''
  const [ready, setReady] = useState(false)
  const clientRef = useRef<MzSubscribeClient | null>(null)
  const sinkRef = useRef<MzSqliteSink | null>(null)
  const wasmRef = useRef<WasmSqlite | null>(null)

  useEffect(() => {
    let cancelled = false
    async function boot() {
      const transport = new MzHttpTransport({ baseUrl })
      const client = new MzSubscribeClient({ transport })
      const wasmDb = new WasmSqlite({ workerUrl: WorkerURL })
      await wasmDb.init()
      const sink = new MzSqliteSink({ db: wasmDb, schema: 'server', serverBaseUrl: baseUrl })
      client.addSink(sink)
      if (!cancelled) {
        clientRef.current = client
        sinkRef.current = sink
        wasmRef.current = wasmDb
        setReady(true)
      } else {
        client.destroy()
        wasmDb.destroy()
      }
    }
    boot()
    return () => {
      cancelled = true
      clientRef.current?.destroy()
      wasmRef.current?.destroy()
    }
  }, [])

  if (!ready || !clientRef.current || !sinkRef.current) {
    return <div style={{ padding: 40, textAlign: 'center', color: '#868e96' }}>Loading SQLite...</div>
  }

  return (
    <MzProvider baseUrl={baseUrl} client={clientRef.current} sqliteSink={sinkRef.current}>
      <Dashboard />
    </MzProvider>
  )
}
