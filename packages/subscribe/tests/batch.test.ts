import { describe, it, expect, vi } from 'vitest'
import { BatchAccumulator } from '../src/batch.js'

describe('BatchAccumulator', () => {
  it('accumulates upserts and seals a batch', () => {
    const acc = new BatchAccumulator('test_view')
    acc.add({ type: 'upsert', key: '1', value: { name: 'alice' } })
    acc.add({ type: 'upsert', key: '2', value: { name: 'bob' } })

    expect(acc.hasPending).toBe(true)
    expect(acc.pendingCount).toBe(2)

    const batch = acc.seal(1000)
    expect(batch.view).toBe('test_view')
    expect(batch.upserts.size).toBe(2)
    expect(batch.upserts.get('1')).toEqual({ name: 'alice' })
    expect(batch.deletes.size).toBe(0)
    expect(batch.progress).toBe(1000)
    expect(batch.kind).toBe('progress')

    // Accumulator is reset after seal
    expect(acc.hasPending).toBe(false)
  })

  it('collapses multiple upserts to the same key', () => {
    const acc = new BatchAccumulator('v')
    acc.add({ type: 'upsert', key: '1', value: { v: 1 } })
    acc.add({ type: 'upsert', key: '1', value: { v: 2 } })
    acc.add({ type: 'upsert', key: '1', value: { v: 3 } })

    const batch = acc.seal(100)
    expect(batch.upserts.size).toBe(1)
    expect(batch.upserts.get('1')).toEqual({ v: 3 })
  })

  it('upsert then delete → only in deletes', () => {
    const acc = new BatchAccumulator('v')
    acc.add({ type: 'upsert', key: '1', value: { v: 1 } })
    acc.add({ type: 'delete', key: '1' })

    const batch = acc.seal(100)
    expect(batch.upserts.size).toBe(0)
    expect(batch.deletes.has('1')).toBe(true)
  })

  it('delete then upsert → only in upserts', () => {
    const acc = new BatchAccumulator('v')
    acc.add({ type: 'delete', key: '1' })
    acc.add({ type: 'upsert', key: '1', value: { v: 'back' } })

    const batch = acc.seal(100)
    expect(batch.upserts.size).toBe(1)
    expect(batch.deletes.size).toBe(0)
  })

  it('seals empty batch when no events', () => {
    const acc = new BatchAccumulator('v')
    expect(acc.hasPending).toBe(false)
    const batch = acc.seal(100)
    expect(batch.upserts.size).toBe(0)
    expect(batch.deletes.size).toBe(0)
  })

  it('snapshot mode sets kind to snapshot', () => {
    const acc = new BatchAccumulator('v')
    acc.setSnapshotMode(true)
    acc.add({ type: 'upsert', key: '1', value: { x: 1 } })
    const batch = acc.seal(100)
    expect(batch.kind).toBe('snapshot')
  })

  it('snapshot mode resets after seal with snapshot kind', () => {
    const acc = new BatchAccumulator('v')
    acc.setSnapshotMode(true)
    acc.add({ type: 'upsert', key: '1', value: { x: 1 } })
    acc.seal(100, 'snapshot')

    acc.add({ type: 'upsert', key: '2', value: { x: 2 } })
    const batch2 = acc.seal(200)
    expect(batch2.kind).toBe('progress') // no longer snapshot
  })

  it('parses string values as JSON', () => {
    const acc = new BatchAccumulator('v')
    acc.add({ type: 'upsert', key: '1', value: '{"name":"alice","age":30}' })
    const batch = acc.seal(100)
    expect(batch.upserts.get('1')).toEqual({ name: 'alice', age: 30 })
  })

  it('handles deletes', () => {
    const acc = new BatchAccumulator('v')
    acc.add({ type: 'delete', key: '1' })
    acc.add({ type: 'delete', key: '2' })

    const batch = acc.seal(100)
    expect(batch.deletes.size).toBe(2)
    expect(batch.deletes.has('1')).toBe(true)
    expect(batch.deletes.has('2')).toBe(true)
  })

  it('skips upserts with malformed JSON string values', () => {
    const acc = new BatchAccumulator('v')
    const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})

    acc.add({ type: 'upsert', key: '1', value: '{not valid json' })
    acc.add({ type: 'upsert', key: '2', value: { name: 'valid' } })

    expect(acc.pendingCount).toBe(1)
    const batch = acc.seal(100)
    expect(batch.upserts.size).toBe(1)
    expect(batch.upserts.get('2')).toEqual({ name: 'valid' })
    expect(batch.upserts.has('1')).toBe(false)
    expect(warnSpy).toHaveBeenCalledOnce()

    warnSpy.mockRestore()
  })
})
