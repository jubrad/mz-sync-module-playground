import { describe, it, expect } from 'vitest'
import { pgTypeToSqlite, inferTypeFromValue, inferSchema, explicitSchema } from '../src/schema.js'

describe('pgTypeToSqlite', () => {
  it('maps integer types', () => {
    expect(pgTypeToSqlite('int4')).toBe('integer')
    expect(pgTypeToSqlite('int8')).toBe('integer')
    expect(pgTypeToSqlite('bool')).toBe('integer')
  })

  it('maps float types', () => {
    expect(pgTypeToSqlite('float4')).toBe('real')
    expect(pgTypeToSqlite('float8')).toBe('real')
    expect(pgTypeToSqlite('numeric')).toBe('real')
  })

  it('defaults to text', () => {
    expect(pgTypeToSqlite('text')).toBe('text')
    expect(pgTypeToSqlite('varchar')).toBe('text')
    expect(pgTypeToSqlite('timestamptz')).toBe('text')
    expect(pgTypeToSqlite('unknown')).toBe('text')
  })
})

describe('inferTypeFromValue', () => {
  it('infers integer from whole numbers', () => {
    expect(inferTypeFromValue(42)).toBe('integer')
  })

  it('infers real from decimals', () => {
    expect(inferTypeFromValue(3.14)).toBe('real')
  })

  it('infers integer from booleans', () => {
    expect(inferTypeFromValue(true)).toBe('integer')
  })

  it('defaults to text', () => {
    expect(inferTypeFromValue('hello')).toBe('text')
    expect(inferTypeFromValue(null)).toBe('text')
    expect(inferTypeFromValue({})).toBe('text')
  })
})

describe('inferSchema', () => {
  it('infers schema from a row with id', () => {
    const schema = inferSchema({ id: 1, name: 'alice', score: 3.14 })
    expect(schema.primaryKey).toBe('id')
    expect(schema.columns).toEqual([
      { name: 'id', type: 'integer' },
      { name: 'name', type: 'text' },
      { name: 'score', type: 'real' },
    ])
  })

  it('respects explicit primaryKey', () => {
    const schema = inferSchema({ key: 'abc', value: 'xyz' }, 'key')
    expect(schema.primaryKey).toBe('key')
    expect(schema.columns[0].name).toBe('key')
  })

  it('adds missing PK column', () => {
    const schema = inferSchema({ name: 'alice' }, 'id')
    expect(schema.columns[0]).toEqual({ name: 'id', type: 'text' })
  })
})

describe('explicitSchema', () => {
  it('builds schema from column map', () => {
    const schema = explicitSchema({ id: 'integer', name: 'text' }, 'id')
    expect(schema.primaryKey).toBe('id')
    expect(schema.columns).toEqual([
      { name: 'id', type: 'integer' },
      { name: 'name', type: 'text' },
    ])
  })

  it('defaults PK to first column', () => {
    const schema = explicitSchema({ foo: 'text', bar: 'integer' })
    expect(schema.primaryKey).toBe('foo')
  })
})
