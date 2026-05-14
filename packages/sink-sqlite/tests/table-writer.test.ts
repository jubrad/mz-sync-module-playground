import { describe, it, expect } from 'vitest'
import { escapeVal, createTableDDL, batchWriteSQL } from '../src/table-writer.js'
import type { TableSchema } from '../src/types.js'

describe('escapeVal', () => {
  it('escapes null/undefined as NULL', () => {
    expect(escapeVal(null)).toBe('NULL')
    expect(escapeVal(undefined)).toBe('NULL')
  })

  it('passes numbers through', () => {
    expect(escapeVal(42)).toBe('42')
    expect(escapeVal(3.14)).toBe('3.14')
  })

  it('converts booleans to 0/1', () => {
    expect(escapeVal(true)).toBe('1')
    expect(escapeVal(false)).toBe('0')
  })

  it('wraps strings in quotes and escapes single quotes', () => {
    expect(escapeVal('hello')).toBe("'hello'")
    expect(escapeVal("it's")).toBe("'it''s'")
    expect(escapeVal("a'b'c")).toBe("'a''b''c'")
  })
})

describe('createTableDDL', () => {
  it('generates valid DDL', () => {
    const schema: TableSchema = {
      columns: [
        { name: 'id', type: 'integer' },
        { name: 'name', type: 'text' },
        { name: 'score', type: 'real' },
      ],
      primaryKey: 'id',
    }
    const ddl = createTableDDL('my_view', schema)
    expect(ddl).toBe(
      'CREATE TABLE IF NOT EXISTS "my_view" ("id" INTEGER, "name" TEXT, "score" REAL, PRIMARY KEY ("id"))',
    )
  })
})

describe('batchWriteSQL', () => {
  const schema: TableSchema = {
    columns: [
      { name: 'id', type: 'integer' },
      { name: 'name', type: 'text' },
      { name: 'active', type: 'integer' },
    ],
    primaryKey: 'id',
  }

  it('generates upserts', () => {
    const upserts = new Map([
      ['1', { name: 'alice', active: true }],
      ['2', { name: 'bob', active: false }],
    ])
    const sql = batchWriteSQL('t', schema, upserts, new Set())
    expect(sql).toContain('BEGIN')
    expect(sql).toContain('COMMIT')
    expect(sql).toContain(`INSERT OR REPLACE INTO "t" ("id","name","active") VALUES ('1','alice',1)`)
    expect(sql).toContain(`INSERT OR REPLACE INTO "t" ("id","name","active") VALUES ('2','bob',0)`)
  })

  it('generates deletes', () => {
    const sql = batchWriteSQL('t', schema, new Map(), new Set(['1', '2']))
    expect(sql).toContain(`DELETE FROM "t" WHERE "id" = '1'`)
    expect(sql).toContain(`DELETE FROM "t" WHERE "id" = '2'`)
  })

  it('handles mixed upserts and deletes', () => {
    const upserts = new Map([['1', { name: 'alice', active: true }]])
    const deletes = new Set(['2'])
    const sql = batchWriteSQL('t', schema, upserts, deletes)
    expect(sql).toContain('INSERT OR REPLACE')
    expect(sql).toContain('DELETE FROM')
  })

  it('handles NULL values', () => {
    const upserts = new Map([['1', { name: null, active: undefined }]])
    const sql = batchWriteSQL('t', schema, upserts as any, new Set())
    expect(sql).toContain('NULL')
  })
})
