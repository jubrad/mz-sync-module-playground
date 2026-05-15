import { describe, it, expect } from 'vitest'
import { MzDirectTransport } from '../src/direct-transport.js'
import { quoteIdent, buildSubscribeSQL } from '../src/direct-stream.js'

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
      {},
      'my_cluster',
    )
    expect(sql).toContain('SET CLUSTER = "my_cluster"')
  })

  it('quotes cluster names with special characters', () => {
    const sql = buildSubscribeSQL(
      { sql: 'SELECT * FROM orders', keyColumn: 'id' },
      {},
      'evil"; DROP TABLE --',
    )
    expect(sql).toContain('SET CLUSTER = "evil""; DROP TABLE --"')
  })

  it('quotes the key column', () => {
    const sql = buildSubscribeSQL(
      { sql: 'SELECT * FROM orders', keyColumn: 'order_id' },
      {},
    )
    expect(sql).toContain('KEY ("order_id")')
  })

  it('quotes key columns with special characters', () => {
    const sql = buildSubscribeSQL(
      { sql: 'SELECT * FROM orders', keyColumn: 'my"col' },
      {},
    )
    expect(sql).toContain('KEY ("my""col")')
  })

  it('defaults key column to "key"', () => {
    const sql = buildSubscribeSQL(
      { sql: 'SELECT * FROM orders' },
      {},
    )
    expect(sql).toContain('KEY ("key")')
  })

  it('includes SNAPSHOT when since is not set', () => {
    const sql = buildSubscribeSQL(
      { sql: 'SELECT * FROM orders', keyColumn: 'id' },
      {},
    )
    expect(sql).toContain('WITH (SNAPSHOT, PROGRESS)')
  })

  it('uses AS OF when since > 0', () => {
    const sql = buildSubscribeSQL(
      { sql: 'SELECT * FROM orders', keyColumn: 'id' },
      { since: 12345 },
    )
    expect(sql).toContain('AS OF 12345')
    expect(sql).not.toContain('SNAPSHOT')
  })

  it('rejects non-safe-integer AS OF values', () => {
    expect(() =>
      buildSubscribeSQL(
        { sql: 'SELECT * FROM orders', keyColumn: 'id' },
        { since: 1.5 },
      ),
    ).toThrow('Invalid since timestamp')
  })

  it('NaN since is treated as no since (falls through to snapshot path)', () => {
    const sql = buildSubscribeSQL(
      { sql: 'SELECT * FROM orders', keyColumn: 'id' },
      { since: NaN },
    )
    // NaN is falsy, so it takes the snapshot path — no AS OF in the SQL
    expect(sql).toContain('WITH (SNAPSHOT, PROGRESS)')
    expect(sql).not.toContain('AS OF')
  })

  it('rejects Infinity AS OF values', () => {
    expect(() =>
      buildSubscribeSQL(
        { sql: 'SELECT * FROM orders', keyColumn: 'id' },
        { since: Infinity },
      ),
    ).toThrow('Invalid since timestamp')
  })

  it('rejects too-large AS OF values', () => {
    expect(() =>
      buildSubscribeSQL(
        { sql: 'SELECT * FROM orders', keyColumn: 'id' },
        { since: Number.MAX_SAFE_INTEGER + 1 },
      ),
    ).toThrow('Invalid since timestamp')
  })
})
