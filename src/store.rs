use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::RwLock;
use serde::Serialize;
use tokio::sync::broadcast;

use crate::error::MzTimestamp;

// ---------------------------------------------------------------------------
// Change events — broadcast to SSE/WS listeners and webhooks
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ChangeEvent {
    Upsert {
        view: String,
        key: String,
        value: String,
        timestamp: MzTimestamp,
    },
    Delete {
        view: String,
        key: String,
        timestamp: MzTimestamp,
    },
    Progress {
        view: String,
        timestamp: MzTimestamp,
    },
}

// ---------------------------------------------------------------------------
// Changelog — replayable log of changes with compaction
// ---------------------------------------------------------------------------

/// A timestamped change for the log.
#[derive(Debug, Clone, Serialize)]
pub struct LogEntry {
    pub timestamp: MzTimestamp,
    pub event: ChangeEvent,
}

/// Ring-buffer changelog. Clients reconnecting with `?since=ts` can replay
/// from here instead of re-fetching the full snapshot. Entries older than
/// `max_entries` are compacted (dropped).
pub struct ChangeLog {
    entries: VecDeque<LogEntry>,
    max_entries: usize,
    /// Oldest timestamp still in the log. 0 if empty.
    min_timestamp: MzTimestamp,
}

impl ChangeLog {
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: VecDeque::with_capacity(max_entries),
            max_entries,
            min_timestamp: 0,
        }
    }

    pub fn push(&mut self, timestamp: MzTimestamp, event: ChangeEvent) {
        self.entries.push_back(LogEntry { timestamp, event });
        // Compact entire epochs if over capacity — never split a progress epoch.
        while self.entries.len() > self.max_entries {
            // Drop entries through the next Progress boundary so replayed
            // epochs are always complete.
            loop {
                match self.entries.pop_front() {
                    Some(LogEntry { event: ChangeEvent::Progress { .. }, .. }) => break,
                    Some(_) => continue,
                    None => break,
                }
            }
        }
        // Update min timestamp.
        self.min_timestamp = self
            .entries
            .front()
            .map(|e| e.timestamp)
            .unwrap_or(0);
    }

    /// Get all entries with timestamp > `since`. Returns None if `since` is
    /// older than our oldest entry (compacted — caller must do a full snapshot).
    pub fn since(&self, since: MzTimestamp) -> Option<Vec<LogEntry>> {
        if since == 0 || (self.min_timestamp > 0 && since < self.min_timestamp) {
            return None; // compacted
        }
        Some(
            self.entries
                .iter()
                .filter(|e| e.timestamp > since)
                .cloned()
                .collect(),
        )
    }

}

// ---------------------------------------------------------------------------
// Per-view store
// ---------------------------------------------------------------------------

/// Column metadata discovered by probing the view's SQL.
#[derive(Debug, Clone, Serialize)]
pub struct ColumnInfo {
    pub name: String,
    pub pg_type: String,
}

/// In-memory state for a single SUBSCRIBE view.
pub struct ViewStore {
    /// Current key-value snapshot.
    pub data: DashMap<String, String>,
    /// Most recent progress timestamp.
    pub progress: AtomicU64,
    /// Broadcast channel for live change events.
    pub tx: broadcast::Sender<ChangeEvent>,
    /// Replayable changelog for reconnecting clients.
    pub changelog: RwLock<ChangeLog>,
    /// View name (for tagging events).
    pub name: String,
    /// Columns shown as summary in live streams.
    pub title_columns: Vec<String>,
    /// Whether the initial snapshot hydration is complete.
    pub hydrated: AtomicU64,
    /// Column metadata discovered at connect time.
    pub columns: RwLock<Vec<ColumnInfo>>,
    /// The key column used for SUBSCRIBE ENVELOPE UPSERT.
    pub key_column: RwLock<String>,
}

/// Default changelog capacity: ~100k entries covers several minutes of
/// high-throughput changes. At 1000 changes/sec this is ~100s of history.
const DEFAULT_CHANGELOG_CAPACITY: usize = 100_000;

impl ViewStore {
    pub fn new(name: String, title_columns: Vec<String>) -> Self {
        let (tx, _) = broadcast::channel(4096);
        Self {
            data: DashMap::new(),
            progress: AtomicU64::new(0),
            tx,
            changelog: RwLock::new(ChangeLog::new(DEFAULT_CHANGELOG_CAPACITY)),
            name,
            title_columns,
            hydrated: AtomicU64::new(0),
            columns: RwLock::new(Vec::new()),
            key_column: RwLock::new(String::new()),
        }
    }

    /// Store column metadata discovered during SUBSCRIBE setup.
    pub fn set_schema(&self, columns: Vec<ColumnInfo>, key_column: String) {
        *self.columns.write() = columns;
        *self.key_column.write() = key_column;
    }

    pub fn apply_upsert(&self, key: String, value: String, timestamp: MzTimestamp) {
        self.data.insert(key.clone(), value.clone());
        let event = ChangeEvent::Upsert {
            view: self.name.clone(),
            key,
            value,
            timestamp,
        };
        self.changelog.write().push(timestamp, event.clone());
        let _ = self.tx.send(event);
    }

    pub fn apply_delete(&self, key: String, timestamp: MzTimestamp) {
        self.data.remove(&key);
        let event = ChangeEvent::Delete {
            view: self.name.clone(),
            key,
            timestamp,
        };
        self.changelog.write().push(timestamp, event.clone());
        let _ = self.tx.send(event);
    }

    pub fn set_progress(&self, timestamp: MzTimestamp) {
        self.progress.store(timestamp, Ordering::Release);
        self.hydrated.store(1, Ordering::Release);
        let event = ChangeEvent::Progress {
            view: self.name.clone(),
            timestamp,
        };
        self.changelog.write().push(timestamp, event.clone());
        let _ = self.tx.send(event);
    }

    pub fn is_hydrated(&self) -> bool {
        self.hydrated.load(Ordering::Acquire) == 1
    }

    pub fn snapshot(&self) -> HashMap<String, String> {
        self.data
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect()
    }

    /// Get changes since a timestamp. Returns `None` if the timestamp is too
    /// old (compacted) — the caller should fall back to a full snapshot.
    pub fn changes_since(&self, since: MzTimestamp) -> Option<Vec<LogEntry>> {
        self.changelog.read().since(since)
    }


    pub fn get(&self, key: &str) -> Option<String> {
        self.data.get(key).map(|v| v.value().clone())
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn current_progress(&self) -> MzTimestamp {
        self.progress.load(Ordering::Acquire)
    }
}

// ---------------------------------------------------------------------------
// Global store — holds all views
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct Store {
    pub views: Arc<DashMap<String, Arc<ViewStore>>>,
}

impl Store {
    pub fn new() -> Self {
        Self {
            views: Arc::new(DashMap::new()),
        }
    }

    pub fn register_view(&self, name: &str, title_columns: Vec<String>) -> Arc<ViewStore> {
        let store = Arc::new(ViewStore::new(name.to_string(), title_columns));
        self.views.insert(name.to_string(), store.clone());
        store
    }

    pub fn get_view(&self, name: &str) -> Option<Arc<ViewStore>> {
        self.views.get(name).map(|v| v.value().clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- Helpers --

    /// Push a complete epoch: `n_upserts` upserts + 1 progress at timestamp `ts`.
    fn push_epoch(log: &mut ChangeLog, ts: MzTimestamp, n_upserts: usize) {
        for i in 0..n_upserts {
            log.push(
                ts,
                ChangeEvent::Upsert {
                    view: "test".into(),
                    key: format!("k{ts}_{i}"),
                    value: format!("v{ts}_{i}"),
                    timestamp: ts,
                },
            );
        }
        log.push(
            ts,
            ChangeEvent::Progress {
                view: "test".into(),
                timestamp: ts,
            },
        );
    }

    fn count_at_ts(entries: &[LogEntry], ts: MzTimestamp) -> usize {
        entries.iter().filter(|e| e.timestamp == ts).count()
    }

    // -----------------------------------------------------------------------
    // ChangeLog: epoch-atomic compaction
    // -----------------------------------------------------------------------

    #[test]
    fn compaction_drops_entire_epoch() {
        // capacity 10, two epochs of 6 entries each → total 12 > 10
        let mut log = ChangeLog::new(10);
        push_epoch(&mut log, 100, 5); // 6
        push_epoch(&mut log, 200, 5); // 6 → 12 > 10

        // epoch@100 fully compacted — any since < 200 returns None
        assert!(log.since(99).is_none());
        assert!(log.since(100).is_none());
        assert_eq!(log.min_timestamp, 200);
        // epoch@200 fully intact in the buffer
        assert_eq!(log.entries.len(), 6);
        assert!(log.entries.iter().all(|e| e.timestamp == 200));
        // Client who saw epoch@200 can continue live
        assert!(log.since(200).is_some());
    }

    #[test]
    fn compaction_never_splits_epoch() {
        // capacity 8 — epoch@100 (4 entries) + epoch@200 (8 entries) = 12 > 8
        // epoch@100 must be dropped entirely, not partially.
        let mut log = ChangeLog::new(8);
        push_epoch(&mut log, 100, 3); // 4
        push_epoch(&mut log, 200, 7); // 8 → total 12

        assert_eq!(log.min_timestamp, 200);
        assert!(log.since(99).is_none());
        assert!(log.since(100).is_none());
        // All 8 entries from epoch@200 present — no partial epoch@100 remnants
        assert_eq!(log.entries.len(), 8);
        assert!(log.entries.iter().all(|e| e.timestamp == 200));
    }

    #[test]
    fn old_per_entry_compaction_would_have_split_epoch() {
        // With the old `pop_front()` one-at-a-time, capacity=10 with epoch@100
        // (4 entries) + epoch@200 (8 entries) = 12 would only pop 2 entries,
        // leaving a partial epoch@100 in the log. The new code drops all of
        // epoch@100 atomically.
        let mut log = ChangeLog::new(10);
        push_epoch(&mut log, 100, 3); // 4 entries
        push_epoch(&mut log, 200, 7); // 8 entries → 12 > 10

        // epoch@100 fully gone — min_timestamp must be 200, not 100
        assert_eq!(log.min_timestamp, 200);
        assert!(log.since(99).is_none());

        // All remaining entries belong to epoch@200
        assert_eq!(log.entries.len(), 8);
        assert!(log.entries.iter().all(|e| e.timestamp == 200));
    }

    #[test]
    fn multiple_epochs_compacted_when_needed() {
        // capacity 5, three epochs of 3 entries each → 9 > 5
        // Must compact two epochs to get back under capacity.
        let mut log = ChangeLog::new(5);
        push_epoch(&mut log, 100, 2); // 3
        push_epoch(&mut log, 200, 2); // 6
        push_epoch(&mut log, 300, 2); // 9 > 5 → compact 100 (→6), still >5 → compact 200 (→3)

        assert!(log.since(99).is_none());
        assert!(log.since(199).is_none());
        assert_eq!(log.min_timestamp, 300);
        // Only epoch@300 remains
        assert_eq!(log.entries.len(), 3);
        assert!(log.entries.iter().all(|e| e.timestamp == 300));
    }

    #[test]
    fn no_compaction_under_capacity() {
        let mut log = ChangeLog::new(20);
        push_epoch(&mut log, 100, 3); // 4
        push_epoch(&mut log, 200, 3); // 8, well under 20

        assert_eq!(log.min_timestamp, 100);
        assert_eq!(log.entries.len(), 8);
        // Client who saw epoch@100 gets epoch@200 via replay
        let entries = log.since(100).unwrap();
        assert_eq!(entries.len(), 4); // 3 upserts + 1 progress for epoch@200
        assert!(entries.iter().all(|e| e.timestamp == 200));
    }

    #[test]
    fn since_zero_always_returns_none() {
        let mut log = ChangeLog::new(100);
        push_epoch(&mut log, 100, 2);
        assert!(log.since(0).is_none());
    }

    #[test]
    fn min_timestamp_advances_correctly() {
        let mut log = ChangeLog::new(10);
        assert_eq!(log.min_timestamp, 0);

        push_epoch(&mut log, 100, 2); // 3 entries
        assert_eq!(log.min_timestamp, 100);

        push_epoch(&mut log, 200, 2); // 6 entries
        assert_eq!(log.min_timestamp, 100);

        // 7 more entries (total 13 > 10) → compact epoch@100 (3 entries) → 10 left
        push_epoch(&mut log, 300, 6);
        assert_eq!(log.min_timestamp, 200);
    }

    #[test]
    fn since_filters_by_timestamp() {
        let mut log = ChangeLog::new(100);
        push_epoch(&mut log, 100, 2); // 3
        push_epoch(&mut log, 200, 3); // 7
        push_epoch(&mut log, 300, 1); // 9

        // since=200 → only epoch@300 (1 upsert + 1 progress)
        let entries = log.since(200).unwrap();
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().all(|e| e.timestamp == 300));

        // since=100 → epoch@200 (4) + epoch@300 (2) = 6
        let entries = log.since(100).unwrap();
        assert_eq!(entries.len(), 6);
    }

    #[test]
    fn compaction_preserves_incomplete_trailing_epoch() {
        // An in-progress epoch (no Progress yet) should survive compaction
        // of the preceding complete epoch.
        let mut log = ChangeLog::new(5);
        push_epoch(&mut log, 100, 2); // 3 entries (complete)
        // 4 upserts for epoch@200, no progress yet
        for i in 0..4 {
            log.push(
                200,
                ChangeEvent::Upsert {
                    view: "test".into(),
                    key: format!("k200_{i}"),
                    value: format!("v200_{i}"),
                    timestamp: 200,
                },
            );
        }
        // total 7 > 5 → compact epoch@100 (through Progress@100)
        assert_eq!(log.min_timestamp, 200);
        assert_eq!(log.entries.len(), 4);
    }

    #[test]
    fn compaction_without_progress_drains_through_buffer() {
        // If the log has only upserts (no Progress events) and exceeds capacity,
        // the compaction loop drains everything since it can't find a Progress
        // boundary. Only entries pushed after the compaction survive.
        let mut log = ChangeLog::new(3);
        for i in 0..5u64 {
            log.push(
                100,
                ChangeEvent::Upsert {
                    view: "test".into(),
                    key: format!("k{i}"),
                    value: format!("v{i}"),
                    timestamp: 100,
                },
            );
        }
        // pushes 1-3 fit; push 4 triggers compaction that drains all 4;
        // push 5 is alone in the buffer.
        assert_eq!(log.entries.len(), 1);
    }

    #[test]
    fn compaction_includes_deletes_in_epoch() {
        // An epoch with mixed upserts and deletes is compacted atomically.
        let mut log = ChangeLog::new(6);
        // epoch@100: upsert + delete + upsert + progress = 4 entries
        log.push(100, ChangeEvent::Upsert {
            view: "test".into(), key: "a".into(), value: "1".into(), timestamp: 100,
        });
        log.push(100, ChangeEvent::Delete {
            view: "test".into(), key: "b".into(), timestamp: 100,
        });
        log.push(100, ChangeEvent::Upsert {
            view: "test".into(), key: "c".into(), value: "3".into(), timestamp: 100,
        });
        log.push(100, ChangeEvent::Progress { view: "test".into(), timestamp: 100 });
        // epoch@200: 3 upserts + progress = 4 entries → total 8 > 6
        push_epoch(&mut log, 200, 3);

        // epoch@100 (including the delete) fully compacted
        assert_eq!(log.min_timestamp, 200);
        assert!(log.since(100).is_none());
        // epoch@200 fully present
        assert_eq!(log.entries.len(), 4);
        assert!(log.entries.iter().all(|e| e.timestamp == 200));
    }

    #[test]
    fn replayed_epochs_are_always_complete() {
        let mut log = ChangeLog::new(15);
        push_epoch(&mut log, 100, 3); // 4
        push_epoch(&mut log, 200, 5); // 10
        push_epoch(&mut log, 300, 4); // 15
        // Trigger compaction of epoch@100 (4 entries) → 14 left
        push_epoch(&mut log, 400, 2); // 18 > 15

        // Client who saw epoch@100 must snapshot
        assert!(log.since(100).is_none());

        // Client who saw epoch@200 can replay — gets complete epoch@300 + epoch@400
        let entries = log.since(200).unwrap();
        assert_eq!(count_at_ts(&entries, 300), 5);
        assert_eq!(count_at_ts(&entries, 400), 3);
        assert_eq!(entries.len(), 8);
        // No stale entries from epoch@100 or epoch@200
        assert_eq!(count_at_ts(&entries, 100), 0);
        assert_eq!(count_at_ts(&entries, 200), 0);
    }

    // -----------------------------------------------------------------------
    // ViewStore integration — changelog via the public API
    // -----------------------------------------------------------------------

    /// Create a ViewStore with a small changelog capacity for testing.
    fn test_viewstore(capacity: usize) -> ViewStore {
        let vs = ViewStore::new("test".into(), vec![]);
        *vs.changelog.write() = ChangeLog::new(capacity);
        vs
    }

    #[test]
    fn viewstore_changes_since_returns_complete_epochs() {
        let vs = test_viewstore(100);

        vs.apply_upsert("a".into(), "1".into(), 100);
        vs.apply_upsert("b".into(), "2".into(), 100);
        vs.set_progress(100);
        vs.apply_upsert("c".into(), "3".into(), 200);
        vs.set_progress(200);

        // since=100 → epoch@200 only (upsert + progress)
        let entries = vs.changes_since(100).unwrap();
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().all(|e| e.timestamp == 200));
    }

    #[test]
    fn viewstore_compacted_since_returns_none() {
        let vs = test_viewstore(5);

        // epoch@100: 2 upserts + progress = 3 entries
        vs.apply_upsert("a".into(), "1".into(), 100);
        vs.apply_upsert("b".into(), "2".into(), 100);
        vs.set_progress(100);
        // epoch@200: 3 upserts + progress = 4 entries → total 7 > 5 → compact epoch@100
        vs.apply_upsert("c".into(), "3".into(), 200);
        vs.apply_upsert("d".into(), "4".into(), 200);
        vs.apply_upsert("e".into(), "5".into(), 200);
        vs.set_progress(200);

        // Pre-compaction timestamps → None
        assert!(vs.changes_since(50).is_none());
        assert!(vs.changes_since(99).is_none());
        assert!(vs.changes_since(100).is_none()); // 100 < min_timestamp(200)

        // Current epoch → Some (empty, since nothing is > 200)
        assert!(vs.changes_since(200).is_some());
    }

    #[test]
    fn viewstore_data_survives_changelog_compaction() {
        let vs = test_viewstore(5);

        vs.apply_upsert("a".into(), "1".into(), 100);
        vs.set_progress(100);
        // Force compaction
        vs.apply_upsert("b".into(), "2".into(), 200);
        vs.apply_upsert("c".into(), "3".into(), 200);
        vs.apply_upsert("d".into(), "4".into(), 200);
        vs.set_progress(200);

        // Changelog compacted past epoch@100 …
        assert!(vs.changes_since(99).is_none());
        // … but the DashMap still has all keys.
        assert_eq!(vs.get("a"), Some("1".into()));
        assert_eq!(vs.get("b"), Some("2".into()));
        assert_eq!(vs.get("c"), Some("3".into()));
        assert_eq!(vs.get("d"), Some("4".into()));
        assert_eq!(vs.len(), 4);
    }

    #[test]
    fn viewstore_snapshot_unaffected_by_compaction() {
        let vs = test_viewstore(5);

        vs.apply_upsert("x".into(), "10".into(), 100);
        vs.apply_upsert("y".into(), "20".into(), 100);
        vs.set_progress(100);

        // Compact epoch@100 away
        vs.apply_upsert("z".into(), "30".into(), 200);
        vs.apply_upsert("w".into(), "40".into(), 200);
        vs.apply_upsert("v".into(), "50".into(), 200);
        vs.set_progress(200);

        let snap = vs.snapshot();
        assert_eq!(snap.len(), 5);
        assert_eq!(snap.get("x").map(|s| s.as_str()), Some("10"));
        assert_eq!(snap.get("z").map(|s| s.as_str()), Some("30"));
    }

    #[test]
    fn viewstore_delete_reflected_in_snapshot_after_compaction() {
        let vs = test_viewstore(5);

        vs.apply_upsert("a".into(), "1".into(), 100);
        vs.set_progress(100);
        // Epoch@200 deletes "a" and adds "b", then forces compaction
        vs.apply_delete("a".into(), 200);
        vs.apply_upsert("b".into(), "2".into(), 200);
        vs.apply_upsert("c".into(), "3".into(), 200);
        vs.set_progress(200);

        assert!(vs.changes_since(99).is_none());
        assert!(vs.get("a").is_none(), "deleted key should be absent");
        assert_eq!(vs.get("b"), Some("2".into()));
        assert_eq!(vs.len(), 2); // b, c
    }
}
