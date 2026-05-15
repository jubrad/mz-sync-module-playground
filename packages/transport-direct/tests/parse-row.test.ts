import { describe, it, expect } from 'vitest'
import { parseSubscribeRow } from '../src/direct-stream.js'

describe('parseSubscribeRow', () => {
  const columns = [
    { name: 'mz_timestamp', type_oid: 0, type_len: 0, type_mod: 0 },
    { name: 'mz_progressed', type_oid: 0, type_len: 0, type_mod: 0 },
    { name: 'mz_state', type_oid: 0, type_len: 0, type_mod: 0 },
    { name: 'id', type_oid: 0, type_len: 0, type_mod: 0 },
    { name: 'name', type_oid: 0, type_len: 0, type_mod: 0 },
    { name: 'status', type_oid: 0, type_len: 0, type_mod: 0 },
  ]

  it('parses upsert row', () => {
    const row = ['1000', 'f', 'upsert', '42', 'alice', 'active']
    const event = parseSubscribeRow(row, columns)
    expect(event).toEqual({
      type: 'upsert',
      key: '42',
      value: { id: '42', name: 'alice', status: 'active' },
      timestamp: 1000,
    })
  })

  it('parses delete row', () => {
    const row = ['2000', 'f', 'delete', '42', null, null]
    const event = parseSubscribeRow(row, columns)
    expect(event).toEqual({
      type: 'delete',
      key: '42',
      timestamp: 2000,
    })
  })

  it('parses progress row (string "t")', () => {
    const row = ['3000', 't', null, null, null, null]
    const event = parseSubscribeRow(row, columns)
    expect(event).toEqual({
      type: 'progress',
      timestamp: 3000,
    })
  })

  it('parses progress row (boolean true)', () => {
    const row = [4000, true, null, null, null, null]
    const event = parseSubscribeRow(row, columns)
    expect(event).toEqual({
      type: 'progress',
      timestamp: 4000,
    })
  })

  it('returns null for short rows (< 3 elements)', () => {
    expect(parseSubscribeRow([1, 2], columns)).toBeNull()
  })

  it('returns null for non-progress rows with < 4 elements', () => {
    // 3 elements: not a progress row but no key column
    expect(parseSubscribeRow(['1000', 'f', 'upsert'], columns)).toBeNull()
  })

  it('returns null for unknown state', () => {
    const row = ['1000', 'f', 'key_violation', '42', 'alice', 'active']
    expect(parseSubscribeRow(row, columns)).toBeNull()
  })
})
