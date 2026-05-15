import { describe, it, expect } from 'vitest'
import { AsyncQueue } from '../src/async-queue.js'

describe('AsyncQueue', () => {
  it('yields pushed items in order', async () => {
    const q = new AsyncQueue<number>()
    q.push(1)
    q.push(2)
    q.push(3)
    q.end()

    const results: number[] = []
    for await (const item of q) {
      results.push(item)
    }
    expect(results).toEqual([1, 2, 3])
  })

  it('waits for items when queue is empty', async () => {
    const q = new AsyncQueue<string>()

    // Push after a delay
    setTimeout(() => {
      q.push('hello')
      q.end()
    }, 10)

    const results: string[] = []
    for await (const item of q) {
      results.push(item)
    }
    expect(results).toEqual(['hello'])
  })

  it('terminates on end()', async () => {
    const q = new AsyncQueue<number>()
    q.end()

    const results: number[] = []
    for await (const item of q) {
      results.push(item)
    }
    expect(results).toEqual([])
  })

  it('ignores pushes after end()', async () => {
    const q = new AsyncQueue<number>()
    q.push(1)
    q.end()
    q.push(2) // should be ignored

    const results: number[] = []
    for await (const item of q) {
      results.push(item)
    }
    expect(results).toEqual([1])
  })

  it('interleaves push and consume', async () => {
    const q = new AsyncQueue<number>()
    const results: number[] = []

    const consumer = (async () => {
      for await (const item of q) {
        results.push(item)
      }
    })()

    q.push(1)
    await new Promise((r) => setTimeout(r, 5))
    q.push(2)
    await new Promise((r) => setTimeout(r, 5))
    q.push(3)
    q.end()

    await consumer
    expect(results).toEqual([1, 2, 3])
  })

  it('throw() rejects a waiting consumer', async () => {
    const q = new AsyncQueue<number>()
    const iter = q[Symbol.asyncIterator]()

    // Start waiting for next item
    const pending = iter.next()

    // Throw an error into the queue
    q.throw(new Error('boom'))

    await expect(pending).rejects.toThrow('boom')
  })

  it('throw() drops buffered items', async () => {
    const q = new AsyncQueue<number>()
    q.push(1)
    q.push(2)
    q.push(3)
    q.throw(new Error('fail'))

    const iter = q[Symbol.asyncIterator]()
    await expect(iter.next()).rejects.toThrow('fail')
  })

  it('throws on concurrent consumers', async () => {
    const q = new AsyncQueue<number>()
    const iter = q[Symbol.asyncIterator]()

    // First consumer starts waiting
    const _pending = iter.next()

    // Second concurrent call should throw synchronously
    expect(() => iter.next()).toThrow('AsyncQueue supports only one concurrent consumer')

    // Clean up
    q.end()
    await _pending
  })

  it('iterator return() terminates the queue', async () => {
    const q = new AsyncQueue<number>()
    q.push(1)
    q.push(2)
    q.push(3)

    const results: number[] = []
    for await (const item of q) {
      results.push(item)
      if (item === 2) break // triggers return()
    }

    expect(results).toEqual([1, 2])

    // Queue should be done after break
    const iter = q[Symbol.asyncIterator]()
    const result = await iter.next()
    expect(result.done).toBe(true)
  })
})
