import type { MzBatch, MzSink, MzSubscribeClient } from '@mz-web/subscribe'

// ── Inline TanStack DB types ────────────────────────────
// Typed inline so we don't need @tanstack/db at compile time.

/** The transaction API that TanStack DB passes to a sync function. */
export interface SyncParams<T> {
  begin(): void
  write(op: { type: 'insert' | 'update' | 'delete'; value: T }): void
  commit(): void
  markReady(): void
  collection: any
}

/**
 * The shape returned by a sync config — TanStack DB expects
 * `{ sync }` where sync is a function receiving SyncParams and
 * returning a cleanup function.
 */
export type SyncFn<T> = (params: SyncParams<T>) => (() => void)

// ── Public API ──────────────────────────────────────────

/**
 * Options for `mzCollectionSync`.
 */
export interface MzCollectionSyncOpts {
  /**
   * The primary key field name used when mapping delete keys.
   * Defaults to `"id"`.
   */
  pk?: string
}

/**
 * Create a TanStack DB sync config for a Materialize view.
 *
 * Returns an object with a `sync` property suitable for spreading
 * into TanStack DB's `createCollection()` options:
 *
 * ```ts
 * const orders = createCollection({
 *   id: 'orders',
 *   ...mzCollectionSync(client, 'orders'),
 * })
 * ```
 *
 * The sync function registers an `MzSink` on the subscribe client
 * that maps each `MzBatch` to a TanStack DB transaction:
 *
 *   begin() -> write(insert|delete)... -> commit()
 *
 * When a snapshot-kind batch arrives, `markReady()` is also called
 * so TanStack DB knows the initial dataset is complete.
 */
export function mzCollectionSync<T extends Record<string, unknown> = Record<string, unknown>>(
  client: MzSubscribeClient,
  viewName: string,
  opts?: MzCollectionSyncOpts,
): { sync: SyncFn<T> } {
  const pk = opts?.pk ?? 'id'

  const sync: SyncFn<T> = (params) => {
    const { begin, write, commit, markReady } = params

    const sink: MzSink = {
      onBatch(batch: MzBatch) {
        // Only process batches for our view
        if (batch.view !== viewName) return

        // Skip empty batches
        if (batch.upserts.size === 0 && batch.deletes.size === 0) return

        begin()

        for (const [, value] of batch.upserts) {
          write({ type: 'insert', value: value as T })
        }

        for (const key of batch.deletes) {
          write({ type: 'delete', value: { [pk]: key } as unknown as T })
        }

        commit()

        // Signal readiness after snapshot completes
        if (batch.kind === 'snapshot') {
          markReady()
        }
      },
    }

    const removeSink = client.addSink(sink)
    return removeSink
  }

  return { sync }
}
