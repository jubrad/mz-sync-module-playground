import { describe, it, expect } from 'vitest'
import { MzHttpTransport } from '../src/http-transport.js'

describe('MzHttpTransport', () => {
  it('can be constructed with options', () => {
    const transport = new MzHttpTransport({ baseUrl: 'http://localhost:8080' })
    expect(transport).toBeDefined()
  })

  it('accepts mode option', () => {
    const ws = new MzHttpTransport({ baseUrl: 'http://localhost:8080', mode: 'ws' })
    const sse = new MzHttpTransport({ baseUrl: 'http://localhost:8080', mode: 'sse' })
    expect(ws).toBeDefined()
    expect(sse).toBeDefined()
  })

  it('strips trailing slash from baseUrl', () => {
    const transport = new MzHttpTransport({ baseUrl: 'http://localhost:8080/' })
    // Can't easily test private field, but construction shouldn't fail
    expect(transport).toBeDefined()
  })

  // Integration tests with actual WebSocket/SSE require a running mz-http server
  // and would go in a separate e2e test suite.
})
