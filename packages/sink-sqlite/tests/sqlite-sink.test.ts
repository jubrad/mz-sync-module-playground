import { describe, it, expect, vi } from 'vitest'
import { MzSqliteSink } from '../src/sqlite-sink.js'
import type { SqliteExec } from '../src/types.js'
import type { MzBatch } from '@mz-web/subscribe'

/** Mock SQLite that captures SQL and returns canned query results. */
function mockDb(): SqliteExec & { execCalls: string[]; queryResults: Map<string, any[]> } {
  const execCalls: string[] = []
  const queryResults = new Map<string, any[]>()
  return {
    execCalls,
    queryResults,
    async exec(sql: string) { execCalls.push(sql) },
    async query<T>(sql: string): Promise<T[]> {
      return (queryResults.get(sql) ?? []) as T[]
    },
  }
}

describe('MzSqliteSink', () => {
  it('creates table on first batch (infer mode)', async () => {
    const db = mockDb()
    const sink = new MzSqliteSink({ db })

    const batch: MzBatch = {
      view: 'test',
      upserts: new Map([['1', { id: 1, name: 'alice' }]]),
      deletes: new Set(),
      progress: 1000,
      kind: 'snapshot',
    }

    await sink.onBatch(batch)

    // Should have CREATE TABLE and BEGIN...COMMIT
    expect(db.execCalls.length).toBe(2)
    expect(db.execCalls[0]).toContain('CREATE TABLE IF NOT EXISTS "test"')
    expect(db.execCalls[1]).toContain('BEGIN')
    expect(db.execCalls[1]).toContain('INSERT OR REPLACE')
    expect(db.execCalls[1]).toContain('COMMIT')
  })

  it('does not recreate table on subsequent batches', async () => {
    const db = mockDb()
    const sink = new MzSqliteSink({ db })

    const batch1: MzBatch = {
      view: 'test',
      upserts: new Map([['1', { id: 1, name: 'alice' }]]),
      deletes: new Set(),
      progress: 1000,
      kind: 'snapshot',
    }
    const batch2: MzBatch = {
      view: 'test',
      upserts: new Map([['2', { id: 2, name: 'bob' }]]),
      deletes: new Set(),
      progress: 2000,
      kind: 'progress',
    }

    await sink.onBatch(batch1)
    await sink.onBatch(batch2)

    // Only one CREATE TABLE
    const creates = db.execCalls.filter((s) => s.includes('CREATE TABLE'))
    expect(creates.length).toBe(1)
  })

  it('tracks version and commit count', async () => {
    const db = mockDb()
    const sink = new MzSqliteSink({ db })

    await sink.onBatch({
      view: 'v', upserts: new Map([['1', { id: 1 }]]),
      deletes: new Set(), progress: 100, kind: 'progress',
    })
    await sink.onBatch({
      view: 'v', upserts: new Map([['2', { id: 2 }]]),
      deletes: new Set(), progress: 200, kind: 'progress',
    })

    expect(sink.version).toBe(2)
    expect(sink.totalCommits).toBe(2)
    expect(sink.totalWrites).toBe(2)
  })

  it('notifies table subscribers on commit', async () => {
    const db = mockDb()
    const sink = new MzSqliteSink({ db })
    const notify = vi.fn()
    sink.subscribeTable('v', notify)

    await sink.onBatch({
      view: 'v', upserts: new Map([['1', { id: 1 }]]),
      deletes: new Set(), progress: 100, kind: 'progress',
    })

    expect(notify).toHaveBeenCalledTimes(1)
  })

  it('unsubscribe stops notifications', async () => {
    const db = mockDb()
    const sink = new MzSqliteSink({ db })
    const notify = vi.fn()
    const unsub = sink.subscribeTable('v', notify)
    unsub()

    await sink.onBatch({
      view: 'v', upserts: new Map([['1', { id: 1 }]]),
      deletes: new Set(), progress: 100, kind: 'progress',
    })

    expect(notify).not.toHaveBeenCalled()
  })

  it('skips empty batches', async () => {
    const db = mockDb()
    const sink = new MzSqliteSink({ db })

    await sink.onBatch({
      view: 'v', upserts: new Map(), deletes: new Set(),
      progress: 100, kind: 'progress',
    })

    expect(db.execCalls.length).toBe(0)
    expect(sink.version).toBe(0)
  })

  it('calls onCommit callback', async () => {
    const db = mockDb()
    const onCommit = vi.fn()
    const sink = new MzSqliteSink({ db, onCommit })

    await sink.onBatch({
      view: 'v', upserts: new Map([['1', { id: 1 }]]),
      deletes: new Set(), progress: 100, kind: 'progress',
    })

    expect(onCommit).toHaveBeenCalledWith('v', 1)
  })

  it('uses explicit schema mode', async () => {
    const db = mockDb()
    const sink = new MzSqliteSink({
      db,
      schema: { columns: { id: 'integer', name: 'text' }, primaryKey: 'id' },
    })

    await sink.onBatch({
      view: 'v', upserts: new Map([['1', { id: 1, name: 'alice' }]]),
      deletes: new Set(), progress: 100, kind: 'progress',
    })

    expect(db.execCalls[0]).toContain('"id" INTEGER')
    expect(db.execCalls[0]).toContain('"name" TEXT')
  })

  it('uses _key as synthetic PK when row has no id field (W3)', async () => {
    const db = mockDb()
    const sink = new MzSqliteSink({ db })

    await sink.onBatch({
      view: 'v',
      upserts: new Map([['mykey', { name: 'alice', score: 42 }]]),
      deletes: new Set(),
      progress: 100,
      kind: 'progress',
    })

    // The CREATE TABLE DDL should reference "_key" as the PK column
    expect(db.execCalls[0]).toContain('"_key"')
    expect(db.execCalls[0]).toContain('PRIMARY KEY ("_key")')
  })

  it('logs warning for delete-only batch with no schema (W2)', async () => {
    const db = mockDb()
    const sink = new MzSqliteSink({ db })
    const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})

    await sink.onBatch({
      view: 'v',
      upserts: new Map(),
      deletes: new Set(['1', '2']),
      progress: 100,
      kind: 'progress',
    })

    expect(warnSpy).toHaveBeenCalledWith(
      expect.stringContaining('dropping 2 delete(s)'),
    )
    // No SQL should have been executed
    expect(db.execCalls.length).toBe(0)
    warnSpy.mockRestore()
  })

  it('logs ROLLBACK failure (W1)', async () => {
    const db = mockDb()
    // Make exec fail on the batch write (second call)
    let callCount = 0
    db.exec = async (sql: string) => {
      callCount++
      if (callCount === 2) throw new Error('write failed')
      if (callCount === 3) throw new Error('rollback failed')
    }
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
    const sink = new MzSqliteSink({ db })

    await sink.onBatch({
      view: 'v',
      upserts: new Map([['1', { id: 1 }]]),
      deletes: new Set(),
      progress: 100,
      kind: 'progress',
    })

    // Should have logged the write error and the rollback failure
    expect(errorSpy).toHaveBeenCalledWith(
      expect.stringContaining('write error'),
      expect.any(Error),
    )
    expect(errorSpy).toHaveBeenCalledWith(
      expect.stringContaining('ROLLBACK failed'),
      expect.any(Error),
    )
    errorSpy.mockRestore()
  })
})
