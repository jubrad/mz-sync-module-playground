/**
 * Push-based queue that implements AsyncIterable.
 * Converts callback-driven APIs (WebSocket.onmessage, EventSource events)
 * into async iterables for consumption by the subscribe client.
 *
 * WARNING: The internal buffer is unbounded. The consumer must keep up with
 * the producer to avoid excessive memory usage.
 */
export class AsyncQueue<T> implements AsyncIterable<T> {
  private queue: T[] = []
  private waiting: { resolve: (result: IteratorResult<T>) => void; reject: (err: unknown) => void } | null = null
  private done = false
  private error: unknown = null

  /** Push an item into the queue. Resolves any waiting consumer. */
  push(item: T): void {
    if (this.done) return
    if (this.waiting) {
      const { resolve } = this.waiting
      this.waiting = null
      resolve({ value: item, done: false })
    } else {
      this.queue.push(item)
    }
  }

  /** Signal that no more items will be pushed. */
  end(): void {
    this.done = true
    if (this.waiting) {
      const { resolve } = this.waiting
      this.waiting = null
      resolve({ value: undefined as unknown as T, done: true })
    }
  }

  /** Signal an error. The waiting consumer is rejected and buffered items are dropped. */
  throw(err: unknown): void {
    this.error = err
    this.done = true
    this.queue.length = 0
    if (this.waiting) {
      const { reject } = this.waiting
      this.waiting = null
      reject(err)
    }
  }

  [Symbol.asyncIterator](): AsyncIterator<T> {
    return {
      next: (): Promise<IteratorResult<T>> => {
        if (this.error) {
          return Promise.reject(this.error)
        }
        if (this.queue.length > 0) {
          return Promise.resolve({ value: this.queue.shift()!, done: false })
        }
        if (this.done) {
          return Promise.resolve({ value: undefined as unknown as T, done: true })
        }
        if (this.waiting) {
          throw new Error('AsyncQueue supports only one concurrent consumer')
        }
        return new Promise((resolve, reject) => {
          this.waiting = { resolve, reject }
        })
      },
      return: (): Promise<IteratorResult<T>> => {
        this.queue.length = 0
        this.end()
        return Promise.resolve({ value: undefined as unknown as T, done: true })
      },
    }
  }
}
