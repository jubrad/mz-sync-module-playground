import { describe, it, expect } from 'vitest'
import { MzDirectTransport } from '../src/direct-transport.js'
import { quoteIdent, buildSubscribeSQL, snapshotBoundary } from '../src/direct-stream.js'

describe('MzDirectTransport', () => {
  const opts = {
    host: 'abc123.materialize.cloud',
    auth: { user: 'test@example.com', password: 'mzp_test' },
  }

  it('can be constructed', () => {
    const transport = new MzDirectTransport(opts)
    expect(transport).toBeDefined()
  })

  it('registerView stores config', () => {
    const transport = new MzDirectTransport(opts)
    transport.registerView('orders', { sql: 'SELECT * FROM orders', keyColumn: 'id' })
    // No error means it stored it
    expect(transport).toBeDefined()
  })

  it('registerViews stores multiple configs', () => {
    const transport = new MzDirectTransport(opts)
    transport.registerViews({
      orders: { sql: 'SELECT * FROM orders', keyColumn: 'id' },
      stores: { sql: 'SELECT * FROM stores', keyColumn: 'id' },
    })
    expect(transport).toBeDefined()
  })

  it('connect throws for unregistered view', () => {
    const transport = new MzDirectTransport(opts)
    expect(() => transport.connect('unknown', { snapshot: true })).toThrow('No config registered')
  })

  it('supports chaining on registerView', () => {
    const transport = new MzDirectTransport(opts)
      .registerView('a', { sql: 'SELECT * FROM a', keyColumn: 'id' })
      .registerView('b', { sql: 'SELECT * FROM b', keyColumn: 'id' })
    expect(transport).toBeDefined()
  })

  it('accepts tls option', () => {
    const transport = new MzDirectTransport({ ...opts, tls: false })
    expect(transport).toBeDefined()
  })

  it('accepts cluster and database options', () => {
    const transport = new MzDirectTransport({
      ...opts,
      cluster: 'default',
      database: 'materialize',
    })
    expect(transport).toBeDefined()
  })
})

describe('quoteIdent', () => {
  it('wraps a simple name in double quotes', () => {
    expect(quoteIdent('my_cluster')).toBe('"my_cluster"')
  })

  it('escapes embedded double quotes', () => {
    expect(quoteIdent('my"cluster')).toBe('"my""cluster"')
  })

  it('escapes multiple double quotes', () => {
    expect(quoteIdent('a"b"c')).toBe('"a""b""c"')
  })

  it('handles names with semicolons (prevents SQL injection)', () => {
    expect(quoteIdent('evil; DROP TABLE')).toBe('"evil; DROP TABLE"')
  })

  it('handles empty string', () => {
    expect(quoteIdent('')).toBe('""')
  })
})

describe('buildSubscribeSQL', () => {
  it('quotes the cluster name', () => {
    const sql = buildSubscribeSQL(
      { sql: 'SELECT * FROM orders', keyColumn: 'id' },
      'my_cluster',
    )
    expect(sql).toContain('SET CLUSTER = "my_cluster"')
  })

  it('quotes cluster names with special characters', () => {
    const sql = buildSubscribeSQL(
      { sql: 'SELECT * FROM orders', keyColumn: 'id' },
      'evil"; DROP TABLE --',
    )
    expect(sql).toContain('SET CLUSTER = "evil""; DROP TABLE --"')
  })

  it('quotes the key column', () => {
    const sql = buildSubscribeSQL(
      { sql: 'SELECT * FROM orders', keyColumn: 'order_id' },
    )
    expect(sql).toContain('KEY ("order_id")')
  })

  it('quotes key columns with special characters', () => {
    const sql = buildSubscribeSQL(
      { sql: 'SELECT * FROM orders', keyColumn: 'my"col' },
    )
    expect(sql).toContain('KEY ("my""col")')
  })

  it('defaults key column to "key"', () => {
    const sql = buildSubscribeSQL(
      { sql: 'SELECT * FROM orders' },
    )
    expect(sql).toContain('KEY ("key")')
  })

  it('always takes a full snapshot and never uses AS OF', () => {
    const sql = buildSubscribeSQL(
      { sql: 'SELECT * FROM orders', keyColumn: 'id' },
    )
    expect(sql).toContain('WITH (SNAPSHOT, PROGRESS)')
    expect(sql).not.toContain('AS OF')
  })
})

describe('snapshotBoundary', () => {
  it('drops the leading progress row and ends the snapshot once progress passes it', () => {
    const boundary = snapshotBoundary()
    const upsert = { type: 'upsert' as const, key: 'a', value: {}, timestamp: 100 }

    expect(boundary({ type: 'progress', timestamp: 100 })).toBeNull()
    expect(boundary(upsert)).toBe(upsert)
    expect(boundary({ type: 'progress', timestamp: 101 })).toEqual({
      type: 'snapshot_complete',
      timestamp: 101,
    })
    expect(boundary({ type: 'progress', timestamp: 102 })).toEqual({
      type: 'progress',
      timestamp: 102,
    })
  })

  it('handles a snapshot with no leading progress row', () => {
    const boundary = snapshotBoundary()
    boundary({ type: 'upsert', key: 'a', value: {}, timestamp: 100 })
    expect(boundary({ type: 'progress', timestamp: 100 })).toBeNull()
    expect(boundary({ type: 'progress', timestamp: 101 })?.type).toBe('snapshot_complete')
  })

  it('completes an empty snapshot', () => {
    const boundary = snapshotBoundary()
    expect(boundary({ type: 'progress', timestamp: 100 })).toBeNull()
    expect(boundary({ type: 'progress', timestamp: 101 })?.type).toBe('snapshot_complete')
  })
})
