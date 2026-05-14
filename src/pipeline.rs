use std::sync::Arc;
use std::time::Duration;

use tracing::{error, info, warn};

use crate::config::ResolvedConnection;
use crate::error::Error;
use crate::store::ViewStore;
use crate::subscribe::{MzSubscriber, SubscribeEvent};

/// Run the subscribe loop for a single view.
///
/// Connects to Materialize, hydrates the in-memory store via SNAPSHOT, then
/// continuously applies changes. Reconnects with exponential backoff on
/// transient failures. Restarts with a full snapshot on timestamp compaction.
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
            Err(Error::TimestampCompacted(ts)) => {
                warn!(
                    view = %view_name,
                    timestamp = ts,
                    "progress compacted; restarting with full snapshot"
                );
                // Clear stale data and restart fresh.
                store.data.clear();
                backoff = Duration::from_secs(1);
                match run_subscribe_fresh(&view_name, &conn, &store, &mut shutdown).await {
                    Ok(()) => return,
                    Err(e) => {
                        error!(view = %view_name, error = %e, debug = ?e, "fresh subscribe failed");
                        tokio::time::sleep(backoff).await;
                    }
                }
            }
            Err(Error::Materialize(ref e)) => {
                error!(
                    view = %view_name,
                    error = %e,
                    debug = ?e,
                    backoff_secs = backoff.as_secs(),
                    "materialize error; reconnecting"
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(max_backoff);
            }
            Err(e) => {
                error!(view = %view_name, error = %e, debug = ?e, "fatal pipeline error; retrying");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(max_backoff);
            }
        }
    }
}

/// Connect using stored progress (or None for first run) and drive the loop.
async fn run_subscribe(
    view_name: &str,
    conn: &ResolvedConnection,
    store: &Arc<ViewStore>,
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<(), Error> {
    let progress = store.current_progress();
    let as_of = if progress > 0 { Some(progress) } else { None };
    info!(view = %view_name, progress = ?as_of, "connecting to Materialize");
    let (source, columns, key_column) = MzSubscriber::connect(conn, as_of).await?;
    store.set_schema(columns, key_column);
    drive_loop(view_name, source, store, shutdown).await
}

/// Connect with no AS OF (full snapshot).
async fn run_subscribe_fresh(
    view_name: &str,
    conn: &ResolvedConnection,
    store: &Arc<ViewStore>,
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<(), Error> {
    info!(view = %view_name, "starting fresh subscribe (full snapshot)");
    let (source, columns, key_column) = MzSubscriber::connect(conn, None).await?;
    store.set_schema(columns, key_column);
    drive_loop(view_name, source, store, shutdown).await
}

/// Core fetch-process loop.
async fn drive_loop(
    view_name: &str,
    mut source: MzSubscriber,
    store: &Arc<ViewStore>,
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<(), Error> {
    let mut pending_ts: Option<u64> = None;

    loop {
        tokio::select! {
            result = source.recv() => {
                let events = result?;
                for event in events {
                    match event {
                        SubscribeEvent::Upsert { key, value } => {
                            store.apply_upsert(key, value, pending_ts.unwrap_or(0));
                        }
                        SubscribeEvent::Delete { key } => {
                            store.apply_delete(key, pending_ts.unwrap_or(0));
                        }
                        SubscribeEvent::Progress(ts) => {
                            store.set_progress(ts);
                            pending_ts = Some(ts);
                            if !store.is_hydrated() {
                                info!(
                                    view = %view_name,
                                    keys = store.len(),
                                    timestamp = ts,
                                    "initial snapshot hydrated"
                                );
                            }
                        }
                    }
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
