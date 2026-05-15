import { describe, it, expect, vi } from 'vitest'
import { useLiveQuery } from '../src/use-live-query.js'
import { useLiveQueryScalar } from '../src/use-live-query-scalar.js'
import { useMzViews } from '../src/use-mz-views.js'
import { useMzKey } from '../src/use-mz-key.js'
import { useFreshness } from '../src/use-freshness.js'
import { MzProvider, useMzContext } from '../src/context.js'

describe('useLiveQuery', () => {
  it('can be imported', () => {
    expect(useLiveQuery).toBeDefined()
    expect(typeof useLiveQuery).toBe('function')
  })
})

describe('useLiveQueryScalar', () => {
  it('can be imported', () => {
    expect(useLiveQueryScalar).toBeDefined()
    expect(typeof useLiveQueryScalar).toBe('function')
  })
})

describe('useMzViews', () => {
  it('can be imported', () => {
    expect(useMzViews).toBeDefined()
    expect(typeof useMzViews).toBe('function')
  })
})

describe('useMzKey', () => {
  it('can be imported', () => {
    expect(useMzKey).toBeDefined()
    expect(typeof useMzKey).toBe('function')
  })
})

describe('useFreshness', () => {
  it('can be imported', () => {
    expect(useFreshness).toBeDefined()
    expect(typeof useFreshness).toBe('function')
  })
})

describe('MzProvider', () => {
  it('can be imported', () => {
    expect(MzProvider).toBeDefined()
    expect(useMzContext).toBeDefined()
  })
})

/**
 * Tests for cancellation / stale-result logic.
 *
 * Since we don't have @testing-library/react or react-dom, we test the
 * cancellation patterns directly using the same async logic the hooks employ.
 */
describe('cancellation logic (C2 + C3)', () => {
  /** Helper that mimics the effect body from useLiveQuery */
  function createQueryEffect(
    sqliteSink: { query: <T>(sql: string, params: unknown[]) => Promise<T[]> },
    sql: string,
    params: unknown[],
  ) {
    let cancelled = false
    let requestId = 0
    let resultRows: unknown[] = []
    let version = 0

    const run = async () => {
      const myId = ++requestId
      if (!sql || !sqliteSink) {
        if (!cancelled) resultRows = []
        return
      }
      const result = await sqliteSink.query(sql, params)
      if (!cancelled && myId === requestId) {
        resultRows = result
        version += 1
      }
    }

    const cancel = () => { cancelled = true }
    const getState = () => ({ rows: resultRows, version, cancelled })

    return { run, cancel, getState }
  }

  it('cancelled effect does not update state after cancellation', async () => {
    let resolveQuery: (value: unknown[]) => void
    const mockSink = {
      query: vi.fn(() => new Promise<unknown[]>((resolve) => { resolveQuery = resolve })),
    }

    const effect = createQueryEffect(mockSink, 'SELECT 1', [])
    // Start the query
    const promise = effect.run()
    // Cancel before query completes (simulates unmount / dependency change)
    effect.cancel()
    // Now resolve the query
    resolveQuery!([{ val: 1 }])
    await promise

    // State should NOT have been updated
    expect(effect.getState().rows).toEqual([])
    expect(effect.getState().version).toBe(0)
  })

  it('stale request ID prevents overwriting fresh results', async () => {
    let resolvers: Array<(value: unknown[]) => void> = []
    const mockSink = {
      query: vi.fn(() => new Promise<unknown[]>((resolve) => { resolvers.push(resolve) })),
    }

    // Simulate two rapid calls (like the subscription callback + params change)
    const effect1 = createQueryEffect(mockSink, 'SELECT 1', [])
    const p1 = effect1.run()

    // The second call bumps the requestId
    const p2 = effect1.run()

    // Resolve the SECOND call first (fast response)
    resolvers[1]([{ val: 'fresh' }])
    await p2

    expect(effect1.getState().rows).toEqual([{ val: 'fresh' }])
    expect(effect1.getState().version).toBe(1)

    // Now resolve the FIRST call (slow, stale response)
    resolvers[0]([{ val: 'stale' }])
    await p1

    // State should still show the fresh result, not the stale one
    expect(effect1.getState().rows).toEqual([{ val: 'fresh' }])
    expect(effect1.getState().version).toBe(1)
  })

  it('non-cancelled effect updates state normally', async () => {
    const mockSink = {
      query: vi.fn().mockResolvedValue([{ id: 1 }, { id: 2 }]),
    }

    const effect = createQueryEffect(mockSink, 'SELECT * FROM t', [42])
    await effect.run()

    expect(effect.getState().rows).toEqual([{ id: 1 }, { id: 2 }])
    expect(effect.getState().version).toBe(1)
    expect(mockSink.query).toHaveBeenCalledWith('SELECT * FROM t', [42])
  })
})
