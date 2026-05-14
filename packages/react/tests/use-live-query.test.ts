import { describe, it, expect } from 'vitest'
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
