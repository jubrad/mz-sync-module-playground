use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use materialize_sdk::{Event, Frontier};
use tracing::{error, info};

use crate::config::ResolvedConnection;
use crate::error::{Error, MzTimestamp};
use crate::store::ViewStore;
use crate::subscribe::{key_change, ViewChange, MzSubscriber};

/// Run the subscribe loop for a single view.
///
/// Connects to Materialize, hydrates the in-memory store from a snapshot, then
/// continuously applies changes. Reconnects with exponential backoff on
/// failure.
///
/// Every connection takes a full snapshot. A view is a query, and a
/// subscription to a query cannot be resumed from a timestamp, so after any
/// break the store is rebuilt from the new snapshot: keys the snapshot no
/// longer contains are deleted, and the store keeps serving its previous
/// contents until the snapshot is complete.
pub async fn run_view(
    view_name: String,
    conn: ResolvedConnection,
    store: Arc<ViewStore>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let mut backoff = Duration::from_secs(1);
    let max_backoff = Duration::from_secs(60);

    loop {
        match run_subscribe(&view_name, &conn, &store, &mut shutdown).await {
            Ok(()) => {
                info!(view = %view_name, "view pipeline shut down gracefully");
                return;
            }
            Err(e) => {
                error!(
                    view = %view_name,
                    error = %e,
                    debug = ?e,
                    backoff_secs = backoff.as_secs(),
                    "pipeline error; reconnecting with a full snapshot"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(max_backoff);
            }
        }
    }
}

/// Connect, take a full snapshot, and drive the loop.
async fn run_subscribe(
    view_name: &str,
    conn: &ResolvedConnection,
    store: &Arc<ViewStore>,
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<(), Error> {
    info!(view = %view_name, "connecting to Materialize (full snapshot)");
    let (source, columns, key_column) = MzSubscriber::connect(conn).await?;
    store.set_schema(columns, key_column);
    drive_loop(view_name, source, store, shutdown).await
}

/// A frontier as the store's progress timestamp. Everything below it has been
/// applied.
pub fn frontier_timestamp(frontier: Frontier) -> MzTimestamp {
    MzTimestamp::try_from(frontier.get()).unwrap_or(MzTimestamp::MAX)
}

/// The state of one connection's stream.
///
/// The SDK releases changes as batches at closed frontiers. Each batch is
/// applied to the store stamped with the frontier that closed it.
#[derive(Default)]
struct Connection {
    /// Snapshot changes that arrived as chunks, before the batch that
    /// completes the snapshot.
    snapshot: Vec<ViewChange>,
}

/// What applying a snapshot did to the store.
struct Hydration {
    timestamp: MzTimestamp,
    /// Keys the store held that the snapshot does not contain.
    removed: usize,
}

impl Connection {
    /// Accepts the next event of the stream. Returns `Some` when the event
    /// completed the snapshot.
    fn handle(&mut self, store: &ViewStore, event: Event) -> Result<Option<Hydration>, Error> {
        let batch = match event {
            // A chunk carries no frontier. Hold it until the snapshot is
            // complete, so the store keeps serving its previous contents.
            Event::SnapshotChunk { updates } => {
                for update in updates {
                    self.snapshot.push(key_change(update)?);
                }
                return Ok(None);
            }
            Event::Batch(batch) => batch,
        };

        let ts = frontier_timestamp(batch.upper());
        let is_snapshot = batch.is_snapshot();
        let mut changes = std::mem::take(&mut self.snapshot);
        for update in batch.into_updates() {
            changes.push(key_change(update)?);
        }

        let stale = if is_snapshot {
            stale_keys(store, &changes)
        } else {
            Vec::new()
        };
        for change in changes {
            match change {
                ViewChange::Upsert { key, value } => store.apply_upsert(key, value, ts),
                ViewChange::Delete { key } => store.apply_delete(key, ts),
            }
        }
        let removed = stale.len();
        for key in stale {
            store.apply_delete(key, ts);
        }
        store.set_progress(ts);

        Ok(is_snapshot.then_some(Hydration {
            timestamp: ts,
            removed,
        }))
    }
}

/// The keys in the store that the snapshot `changes` do not contain. They were
/// deleted while no subscription was running.
fn stale_keys(store: &ViewStore, changes: &[ViewChange]) -> Vec<String> {
    let mut live: HashSet<&str> = HashSet::new();
    for change in changes {
        match change {
            ViewChange::Upsert { key, .. } => {
                live.insert(key);
            }
            ViewChange::Delete { key } => {
                live.remove(key.as_str());
            }
        }
    }
    store
        .data
        .iter()
        .filter(|entry| !live.contains(entry.key().as_str()))
        .map(|entry| entry.key().clone())
        .collect()
}

/// Core fetch-process loop.
async fn drive_loop(
    view_name: &str,
    mut source: MzSubscriber,
    store: &Arc<ViewStore>,
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<(), Error> {
    let mut connection = Connection::default();

    loop {
        tokio::select! {
            event = source.next() => {
                let Some(event) = event? else {
                    // The view can never change again, as with a constant
                    // query. Keep serving it.
                    info!(view = %view_name, "subscription ended; the view is final");
                    while !*shutdown.borrow() {
                        if shutdown.changed().await.is_err() {
                            break;
                        }
                    }
                    return Ok(());
                };
                if let Some(hydration) = connection.handle(store, event)? {
                    info!(
                        view = %view_name,
                        keys = store.len(),
                        removed = hydration.removed,
                        timestamp = hydration.timestamp,
                        "snapshot hydrated"
                    );
                }
            }
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    info!(view = %view_name, "shutdown signal received");
                    return Ok(());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use materialize_sdk::core::{Batcher, Message, Start};
    use materialize_sdk::{Change, Update};

    use super::*;

    fn upsert(key: &str, value: &str, timestamp: u64) -> Message {
        Message::Update(Update {
            timestamp,
            change: Change::Upsert {
                key: vec![Some(key.into())],
                value: vec![Some(value.into())],
            },
        })
    }

    fn delete(key: &str, timestamp: u64) -> Message {
        Message::Update(Update {
            timestamp,
            change: Change::Delete {
                key: vec![Some(key.into())],
            },
        })
    }

    fn progress(frontier: u64) -> Message {
        Message::Progress { frontier }
    }

    fn store() -> ViewStore {
        ViewStore::new("test".into(), vec![])
    }

    /// One connection: the SDK's batcher in front of our handler, as
    /// `LiveSubscription` runs it.
    struct Stream {
        batcher: Batcher,
        connection: Connection,
    }

    impl Stream {
        fn new() -> Stream {
            Stream {
                batcher: Batcher::new(Start::Snapshot { as_of: None }, None),
                connection: Connection::default(),
            }
        }

        fn push(&mut self, store: &ViewStore, message: Message) -> Option<Hydration> {
            let event = self.batcher.push(message).unwrap()?;
            self.connection.handle(store, event).unwrap()
        }
    }

    #[test]
    fn snapshot_is_applied_only_once_progress_passes_it() {
        let store = store();
        let mut stream = Stream::new();

        assert!(stream.push(&store, progress(100)).is_none());
        assert!(stream.push(&store, upsert("a", "1", 100)).is_none());
        assert!(stream.push(&store, upsert("b", "2", 100)).is_none());
        assert_eq!(store.len(), 0);
        assert!(!store.is_hydrated());
        assert_eq!(store.current_progress(), 0);

        let hydration = stream.push(&store, progress(101)).unwrap();
        assert_eq!(hydration.timestamp, 101);
        assert_eq!(hydration.removed, 0);
        assert_eq!(store.len(), 2);
        assert!(store.is_hydrated());
        assert_eq!(store.current_progress(), 101);
    }

    #[test]
    fn changes_are_stamped_with_the_frontier_that_closes_them() {
        let store = store();
        let mut stream = Stream::new();
        stream.push(&store, progress(100));
        stream.push(&store, upsert("a", "1", 100));
        stream.push(&store, progress(101));

        stream.push(&store, upsert("b", "2", 150));
        assert_eq!(store.len(), 1);
        assert!(stream.push(&store, progress(200)).is_none());
        assert_eq!(store.len(), 2);

        // A client that has everything through 101 replays exactly the epoch
        // closed at 200: the upsert and its progress.
        let entries = store.changes_since(101).unwrap();
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().all(|e| e.timestamp == 200));
    }

    #[test]
    fn reconnect_snapshot_removes_keys_deleted_while_disconnected() {
        let store = store();
        let mut first = Stream::new();
        first.push(&store, progress(100));
        first.push(&store, upsert("a", "1", 100));
        first.push(&store, upsert("b", "2", 100));
        first.push(&store, upsert("c", "3", 100));
        first.push(&store, progress(101));

        // The connection breaks. `b` is deleted and `c` changes before the
        // next one starts.
        let mut second = Stream::new();
        second.push(&store, progress(500));
        second.push(&store, upsert("a", "1", 500));
        second.push(&store, upsert("c", "30", 500));
        // The store keeps serving the old contents until the snapshot is in.
        assert_eq!(store.get("b").as_deref(), Some("2"));
        assert_eq!(store.current_progress(), 101);

        let hydration = second.push(&store, progress(501)).unwrap();
        assert_eq!(hydration.removed, 1);
        assert_eq!(store.get("a").as_deref(), Some("1"));
        assert_eq!(store.get("b"), None);
        assert_eq!(store.get("c").as_deref(), Some("30"));
        assert_eq!(store.current_progress(), 501);

        // A client resuming from before the break is sent the delete.
        let entries = store.changes_since(101).unwrap();
        assert!(entries.iter().any(|e| matches!(
            &e.event,
            crate::store::ChangeEvent::Delete { key, .. } if key == "b"
        )));
    }

    #[test]
    fn reconnect_to_an_empty_relation_clears_the_store() {
        let store = store();
        let mut first = Stream::new();
        first.push(&store, progress(100));
        first.push(&store, upsert("a", "1", 100));
        first.push(&store, progress(101));

        let mut second = Stream::new();
        second.push(&store, progress(500));
        let hydration = second.push(&store, progress(501)).unwrap();
        assert_eq!(hydration.removed, 1);
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn a_delete_after_the_snapshot_row_wins_within_the_first_batch() {
        let store = store();
        let mut stream = Stream::new();
        stream.push(&store, progress(100));
        stream.push(&store, upsert("a", "1", 100));
        stream.push(&store, delete("a", 105));
        stream.push(&store, progress(110));
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn snapshot_chunks_are_held_until_the_snapshot_completes() {
        let store = store();
        store.apply_upsert("stale".into(), "0".into(), 1);
        let mut connection = Connection::default();

        let chunk = Event::SnapshotChunk {
            updates: vec![Update {
                timestamp: 100,
                change: Change::Upsert {
                    key: vec![Some("a".into())],
                    value: vec![Some("1".into())],
                },
            }],
        };
        assert!(connection.handle(&store, chunk).unwrap().is_none());
        assert_eq!(store.get("a"), None);
        assert_eq!(store.get("stale").as_deref(), Some("0"));

        // The batch that completes the snapshot, with the rest of it.
        let mut batcher = Batcher::new(Start::Snapshot { as_of: None }, None);
        batcher.push(progress(100)).unwrap();
        batcher.push(upsert("b", "2", 100)).unwrap();
        let last = batcher.push(progress(101)).unwrap().unwrap();
        let hydration = connection.handle(&store, last).unwrap().unwrap();

        assert_eq!(hydration.removed, 1);
        assert_eq!(store.get("a").as_deref(), Some("1"));
        assert_eq!(store.get("b").as_deref(), Some("2"));
        assert_eq!(store.get("stale"), None);
    }
}
