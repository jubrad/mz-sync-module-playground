/** Materialize progress timestamp (milliseconds since epoch). */
export type MzTimestamp = number

/** Wire event from a Materialize SUBSCRIBE stream. */
export type MzEvent =
  | { type: 'upsert'; key: string; value: Record<string, unknown> | string; timestamp?: MzTimestamp }
  | { type: 'delete'; key: string; timestamp?: MzTimestamp }
  | { type: 'progress'; timestamp: MzTimestamp }
  | { type: 'snapshot_complete'; timestamp: MzTimestamp }
  | { type: 'catchup_complete'; timestamp: MzTimestamp }
  | { type: 'lagged' }

/**
 * A committed batch of changes between two progress boundaries.
 * This is the atomic unit of work that sinks consume.
 *
 * Upserts within a batch are collapsed: if a key is upserted multiple times,
 * only the last value is kept. If a key is upserted then deleted, it appears
 * only in `deletes`.
 *
 * **Important:** Batches must not be mutated by sinks. The same batch instance
 * is delivered to all registered sinks. Mutating it (e.g. modifying the
 * `upserts` map or `deletes` set) will corrupt data for other sinks and for
 * the subscribe client itself. If a sink needs to transform the data, it
 * should make a copy first.
 */
export interface MzBatch {
  /** View name this batch belongs to. */
  view: string
  /** Key → value for rows that were inserted or updated. */
  upserts: Map<string, Record<string, unknown>>
  /** Keys that were deleted. */
  deletes: Set<string>
  /** Progress timestamp that sealed this batch. */
  progress: MzTimestamp
  /** What triggered this batch. */
  kind: 'snapshot' | 'catchup' | 'progress'
}

/** Options for connecting to a view. */
export interface ConnectOpts {
  /** Resume from this progress timestamp. Undefined/0 = fresh snapshot. */
  since?: MzTimestamp
  /** If true, request a full snapshot regardless of `since`. */
  snapshot?: boolean
}

/** Per-view metadata exposed to consumers. */
export interface ViewHandle {
  readonly view: string
  readonly progress: MzTimestamp
  readonly ready: boolean
  readonly connected: boolean
}
