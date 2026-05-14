/**
 * Push-based queue that implements AsyncIterable.
 * Converts callback-driven APIs (WebSocket.onmessage, EventSource events)
 * into async iterables for consumption by the subscribe client.
 */
export class AsyncQueue<T> implements AsyncIterable<T> {
  private queue: T[] = []
  private waiting: ((result: IteratorResult<T>) => void) | null = null
  private done = false
  private error: unknown = null

  /** Push an item into the queue. Resolves any waiting consumer. */
  push(item: T): void {
    if (this.done) return
    if (this.waiting) {
      const resolve = this.waiting
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
      const resolve = this.waiting
      this.waiting = null
      resolve({ value: undefined as unknown as T, done: true })
    }
  }

  /** Signal an error. The next pull will reject. */
  throw(err: unknown): void {
    this.error = err
    this.end()
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
        return new Promise((resolve) => {
          this.waiting = resolve
        })
      },
    }
  }
}
