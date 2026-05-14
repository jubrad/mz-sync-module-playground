import type { MzBatch, MzEvent, MzTimestamp } from './types.js'

/**
 * Accumulates subscribe events between progress boundaries and emits
 * atomic batches.
 *
 * Events are collapsed within a batch:
 * - Multiple upserts to the same key → last value wins
 * - Upsert then delete for the same key → only in deletes
 * - Delete then upsert for the same key → only in upserts
 */
export class BatchAccumulator {
  private upserts = new Map<string, Record<string, unknown>>()
  private deletes = new Set<string>()
  private isSnapshot = false

  constructor(private view: string) {}

  /** Mark this accumulator as collecting a snapshot (affects batch.kind). */
  setSnapshotMode(snapshot: boolean) {
    this.isSnapshot = snapshot
  }

  /** Add an event to the current batch. */
  add(event: MzEvent): void {
    switch (event.type) {
      case 'upsert': {
        const val = typeof event.value === 'string'
          ? JSON.parse(event.value) as Record<string, unknown>
          : event.value
        this.upserts.set(event.key, val)
        this.deletes.delete(event.key)
        break
      }
      case 'delete': {
        this.deletes.add(event.key)
        this.upserts.delete(event.key)
        break
      }
      // progress, snapshot_complete, catchup_complete, lagged are not accumulated
    }
  }

  /** Seal the batch and return it. Resets the accumulator for the next batch. */
  seal(progress: MzTimestamp, kind?: MzBatch['kind']): MzBatch {
    const batch: MzBatch = {
      view: this.view,
      upserts: this.upserts,
      deletes: this.deletes,
      progress,
      kind: kind ?? (this.isSnapshot ? 'snapshot' : 'progress'),
    }

    // Reset for next batch
    this.upserts = new Map()
    this.deletes = new Set()
    if (kind === 'snapshot' || kind === 'catchup') {
      this.isSnapshot = false
    }

    return batch
  }

  /** Whether there are any pending changes. */
  get hasPending(): boolean {
    return this.upserts.size > 0 || this.deletes.size > 0
  }

  /** Number of pending operations. */
  get pendingCount(): number {
    return this.upserts.size + this.deletes.size
  }
}
