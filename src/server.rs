use std::collections::HashMap;
use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Json};
use axum::routing::{delete, get, post};
use axum::Router;
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, RwLock};
use tokio_stream::wrappers::{BroadcastStream, ReceiverStream};
use tokio_stream::StreamExt;
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;

use crate::config::ResolvedConnection;
use crate::error::MzTimestamp;
use crate::store::{ChangeEvent, Store, ViewStore};
use crate::subscribe::{MzSubscriber, SubscribeEvent};

// ---------------------------------------------------------------------------
// Webhook registry
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct WebhookEntry {
    pub id: String,
    pub url: String,
    #[serde(skip_serializing)]
    pub secret: Option<String>,
    pub views: Vec<String>,
}

pub type WebhookRegistry = Arc<RwLock<Vec<WebhookEntry>>>;

// ---------------------------------------------------------------------------
// Webhook test echo — stores received payloads for the demo UI
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct WebhookTestPayload {
    received_at: String,
    signature: Option<String>,
    body: serde_json::Value,
}

pub type WebhookTestLog = Arc<RwLock<Vec<WebhookTestPayload>>>;

// ---------------------------------------------------------------------------
// App state
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct AppState {
    pub store: Store,
    pub connections: Arc<HashMap<String, ResolvedConnection>>,
    pub webhooks: WebhookRegistry,
    pub webhook_test_log: WebhookTestLog,
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

pub fn build_router(state: AppState, static_dir: Option<String>) -> Router {
    let api = Router::new()
        .route("/api/views", get(list_views))
        .route("/health", get(health))
        .route("/api/views/:name/schema", get(get_view_schema))
        .route("/api/views/:name", get(get_view_data))
        .route("/api/views/:name/list", get(list_view_objects))
        .route("/api/views/:name/:key", get(get_view_key))
        .route("/api/sse/:name", get(view_sse))
        .route("/api/ws/:name", get(view_ws))
        .route("/api/webhooks", get(list_webhooks))
        .route("/api/webhooks", post(register_webhook))
        .route("/api/webhooks/:id", delete(unregister_webhook))
        .route("/api/webhook-test", get(list_webhook_test))
        .route("/api/webhook-test", post(receive_webhook_test))
        .route("/api/webhook-test", delete(clear_webhook_test))
        .layer(CorsLayer::permissive())
        .with_state(state);

    if let Some(dir) = static_dir {
        api.fallback_service(ServeDir::new(dir))
    } else {
        api
    }
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct ViewInfo {
    name: String,
    keys: usize,
    hydrated: bool,
    progress_timestamp: u64,
    title_columns: Vec<String>,
}

#[derive(Serialize)]
struct ViewsResponse {
    server_time_ms: u64,
    views: Vec<ViewInfo>,
}

async fn list_views(State(state): State<AppState>) -> Json<ViewsResponse> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let mut views: Vec<ViewInfo> = state
        .store
        .views
        .iter()
        .map(|entry| {
            let vs = entry.value();
            ViewInfo {
                name: vs.name.clone(),
                keys: vs.len(),
                hydrated: vs.is_hydrated(),
                progress_timestamp: vs.current_progress(),
                title_columns: vs.title_columns.clone(),
            }
        })
        .collect();
    views.sort_by(|a, b| a.name.cmp(&b.name));
    Json(ViewsResponse { server_time_ms: now, views })
}

async fn health() -> &'static str {
    "ok"
}

#[derive(Serialize)]
struct SchemaResponse {
    name: String,
    key_column: String,
    columns: Vec<crate::store::ColumnInfo>,
}

async fn get_view_schema(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<SchemaResponse>, StatusCode> {
    let vs = state.store.get_view(&name).ok_or(StatusCode::NOT_FOUND)?;
    let columns = vs.columns.read().clone();
    let key_column = vs.key_column.read().clone();
    if columns.is_empty() {
        return Err(StatusCode::SERVICE_UNAVAILABLE); // not hydrated yet
    }
    Ok(Json(SchemaResponse {
        name: vs.name.clone(),
        key_column,
        columns,
    }))
}

async fn get_view_data(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<HashMap<String, serde_json::Value>>, StatusCode> {
    let vs = state.store.get_view(&name).ok_or(StatusCode::NOT_FOUND)?;
    let snap = vs.snapshot();
    let out: HashMap<String, serde_json::Value> = snap
        .into_iter()
        .map(|(k, v)| {
            let val = serde_json::from_str(&v).unwrap_or(serde_json::Value::String(v));
            (k, val)
        })
        .collect();
    Ok(Json(out))
}

async fn get_view_key(
    State(state): State<AppState>,
    Path((name, key)): Path<(String, String)>,
) -> Result<impl IntoResponse, StatusCode> {
    let vs = state.store.get_view(&name).ok_or(StatusCode::NOT_FOUND)?;
    let value = vs.get(&key).ok_or(StatusCode::NOT_FOUND)?;
    match serde_json::from_str::<serde_json::Value>(&value) {
        Ok(v) => Ok(Json(v).into_response()),
        Err(_) => Ok((StatusCode::OK, value).into_response()),
    }
}

/// List objects in a view with optional search, sort, and pagination.
/// Query params: ?q=search&limit=50&offset=0&sort=key|value
async fn list_view_objects(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let vs = state.store.get_view(&name).ok_or(StatusCode::NOT_FOUND)?;
    let snap = vs.snapshot();

    let q = params.get("q").map(|s| s.to_lowercase());
    let limit: usize = params
        .get("limit")
        .and_then(|s| s.parse().ok())
        .unwrap_or(100);
    let offset: usize = params
        .get("offset")
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let sort_by = params.get("sort").map(|s| s.as_str()).unwrap_or("key");

    let mut items: Vec<(String, serde_json::Value)> = snap
        .into_iter()
        .filter(|(k, v)| {
            if let Some(ref q) = q {
                k.to_lowercase().contains(q) || v.to_lowercase().contains(q)
            } else {
                true
            }
        })
        .map(|(k, v)| {
            let val = serde_json::from_str(&v).unwrap_or(serde_json::Value::String(v));
            (k, val)
        })
        .collect();

    match sort_by {
        "value" => items.sort_by(|a, b| a.1.to_string().cmp(&b.1.to_string())),
        _ => items.sort_by(|a, b| a.0.cmp(&b.0)),
    }

    let total = items.len();
    let page: Vec<serde_json::Value> = items
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(|(k, v)| serde_json::json!({ "key": k, "value": v }))
        .collect();

    Ok(Json(serde_json::json!({
        "view": name,
        "total": total,
        "offset": offset,
        "limit": limit,
        "count": page.len(),
        "progress_timestamp": vs.current_progress(),
        "items": page,
    })))
}

/// Boxed SSE stream (needed to unify different stream types across branches).
type SseStream = Pin<Box<dyn tokio_stream::Stream<Item = Result<Event, Infallible>> + Send>>;

/// SSE stream. Supports three modes:
///   ?snapshot=true     — full snapshot then live
///   ?since=<timestamp> — replay changelog since ts, then live (snapshots from Materialize if compacted)
///   (neither)          — live only
async fn view_sse(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Sse<SseStream>, StatusCode> {
    let vs = state.store.get_view(&name).ok_or(StatusCode::NOT_FOUND)?;

    let since: Option<MzTimestamp> = params.get("since").and_then(|s| s.parse().ok());
    let compacted = since
        .map(|ts| vs.changes_since(ts).is_none())
        .unwrap_or(false);

    if compacted {
        // Client's since is past the compacted frontier — snapshot from Materialize.
        let conn = state
            .connections
            .get(&name)
            .cloned()
            .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
        let broadcast_rx = vs.tx.subscribe();
        let view_name = name.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(256);

        tokio::spawn(async move {
            stream_mz_snapshot(conn, view_name, broadcast_rx, tx).await;
        });

        let stream: SseStream = Box::pin(ReceiverStream::new(rx));
        return Ok(Sse::new(stream).keep_alive(
            KeepAlive::new()
                .interval(Duration::from_secs(15))
                .text("ping"),
        ));
    }

    // Non-compacted path: changelog replay, force snapshot from local, or live-only.
    // Subscribe to broadcast BEFORE building the catchup — ensures no gap.
    let rx = vs.tx.subscribe();
    let view_name = name.clone();

    let catchup_events = build_catchup_events(&vs, &view_name, &params);

    let catchup_stream =
        tokio_stream::iter(catchup_events.into_iter().map(Ok::<_, Infallible>));

    let live_stream = BroadcastStream::new(rx).filter_map(move |result| match result {
        Ok(event) => {
            let event_type = match &event {
                ChangeEvent::Upsert { .. } => "upsert",
                ChangeEvent::Delete { .. } => "delete",
                ChangeEvent::Progress { .. } => "progress",
            };
            Some(Ok(Event::default()
                .event(event_type)
                .json_data(&event)
                .unwrap()))
        }
        Err(_) => None,
    });

    let stream: SseStream = Box::pin(catchup_stream.chain(live_stream));

    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("ping"),
    ))
}

/// Build the initial catchup events based on query params.
fn build_catchup_events(
    vs: &ViewStore,
    view_name: &str,
    params: &HashMap<String, String>,
) -> Vec<Event> {
    let since: Option<MzTimestamp> = params
        .get("since")
        .and_then(|s| s.parse().ok());

    let force_snapshot = params
        .get("snapshot")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);

    // Try changelog replay first.
    if let Some(since_ts) = since {
        if let Some(entries) = vs.changes_since(since_ts) {
            let mut events: Vec<Event> = entries
                .into_iter()
                .map(|entry| {
                    let event_type = match &entry.event {
                        ChangeEvent::Upsert { .. } => "upsert",
                        ChangeEvent::Delete { .. } => "delete",
                        ChangeEvent::Progress { .. } => "progress",
                    };
                    Event::default()
                        .event(event_type)
                        .json_data(&entry.event)
                        .unwrap()
                })
                .collect();
            events.push(
                Event::default()
                    .event("catchup_complete")
                    .data(serde_json::json!({
                        "mode": "changelog",
                        "since": since_ts,
                        "events": events.len()
                    }).to_string()),
            );
            return events;
        }
        // Changelog compacted — fall through to full snapshot.
    }

    if force_snapshot || since.is_some() {
        // Full snapshot.
        let snap = vs.snapshot();
        let ts = vs.current_progress();
        let mut events: Vec<Event> = snap
            .into_iter()
            .map(|(k, v)| {
                let ce = ChangeEvent::Upsert {
                    view: view_name.to_string(),
                    key: k,
                    value: v,
                    timestamp: ts,
                };
                Event::default()
                    .event("upsert")
                    .json_data(&ce)
                    .unwrap()
            })
            .collect();
        events.push(
            Event::default()
                .event("catchup_complete")
                .data(serde_json::json!({
                    "mode": "snapshot",
                    "keys": events.len(),
                    "progress": ts
                }).to_string()),
        );
        events
    } else {
        vec![]
    }
}

/// Build catchup as raw JSON strings (for WebSocket).
fn build_ws_catchup(
    vs: &ViewStore,
    view_name: &str,
    params: &HashMap<String, String>,
) -> Vec<String> {
    let since: Option<MzTimestamp> = params.get("since").and_then(|s| s.parse().ok());
    let force_snapshot = params.get("snapshot").map(|v| v == "true" || v == "1").unwrap_or(false);

    if let Some(since_ts) = since {
        if let Some(entries) = vs.changes_since(since_ts) {
            let mut msgs: Vec<String> = entries
                .into_iter()
                .map(|e| serde_json::to_string(&e.event).unwrap_or_default())
                .collect();
            msgs.push(serde_json::json!({
                "type": "catchup_complete", "mode": "changelog",
                "since": since_ts, "events": msgs.len()
            }).to_string());
            return msgs;
        }
    }

    if force_snapshot || since.is_some() {
        let snap = vs.snapshot();
        let ts = vs.current_progress();
        let mut msgs: Vec<String> = snap
            .into_iter()
            .map(|(k, v)| {
                serde_json::to_string(&ChangeEvent::Upsert {
                    view: view_name.to_string(), key: k, value: v, timestamp: ts,
                }).unwrap_or_default()
            })
            .collect();
        msgs.push(serde_json::json!({
            "type": "catchup_complete", "mode": "snapshot",
            "keys": msgs.len(), "progress": ts
        }).to_string());
        msgs
    } else {
        vec![]
    }
}

// ---------------------------------------------------------------------------
// Materialize snapshot for stale clients
// ---------------------------------------------------------------------------

/// Result of fetching a fresh snapshot from Materialize.
struct MzSnapshot {
    keys: Vec<(String, String)>,
    timestamp: MzTimestamp,
    key_count: usize,
}

/// Connect to Materialize and read the snapshot phase of a SUBSCRIBE.
/// Returns the snapshot key-values and the progress timestamp.
async fn fetch_mz_snapshot(
    conn: &ResolvedConnection,
    view_name: &str,
) -> Result<MzSnapshot, crate::error::Error> {
    tracing::info!(view = %view_name, "fetching Mz snapshot for stale client");
    let (mut sub, _, _) = MzSubscriber::connect(conn, None).await?;
    let mut keys: HashMap<String, String> = HashMap::new();

    let snap_ts = 'snapshot: loop {
        let events = sub.recv().await?;
        for event in events {
            match event {
                SubscribeEvent::Upsert { key, value } => {
                    keys.insert(key, value);
                }
                SubscribeEvent::Delete { key } => {
                    keys.remove(&key);
                }
                SubscribeEvent::Progress(ts) => {
                    break 'snapshot ts;
                }
            }
        }
    };

    tracing::info!(
        view = %view_name,
        keys = keys.len(),
        timestamp = snap_ts,
        "Mz snapshot complete for client"
    );

    let key_count = keys.len();
    Ok(MzSnapshot {
        keys: keys.into_iter().collect(),
        timestamp: snap_ts,
        key_count,
    })
}

/// Spawned task: snapshot a view from Materialize for a single SSE client,
/// then transition to the shared broadcast stream.
async fn stream_mz_snapshot(
    conn: ResolvedConnection,
    view_name: String,
    mut broadcast_rx: broadcast::Receiver<ChangeEvent>,
    tx: tokio::sync::mpsc::Sender<Result<Event, Infallible>>,
) {
    // Phase 1: Snapshot from Materialize.
    let snapshot = match fetch_mz_snapshot(&conn, &view_name).await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, view = %view_name, "client Mz snapshot failed");
            let err = Event::default()
                .event("error")
                .data(serde_json::json!({"message": "snapshot failed"}).to_string());
            let _ = tx.send(Ok(err)).await;
            return;
        }
    };

    let snap_ts = snapshot.timestamp;
    let key_count = snapshot.key_count;

    for (key, value) in snapshot.keys {
        let ce = ChangeEvent::Upsert {
            view: view_name.clone(),
            key,
            value,
            timestamp: snap_ts,
        };
        let sse = Event::default().event("upsert").json_data(&ce).unwrap();
        if tx.send(Ok(sse)).await.is_err() {
            return;
        }
    }

    let complete = Event::default()
        .event("catchup_complete")
        .data(
            serde_json::json!({
                "mode": "snapshot",
                "keys": key_count,
                "progress": snap_ts,
                "source": "materialize"
            })
            .to_string(),
        );
    if tx.send(Ok(complete)).await.is_err() {
        return;
    }

    // Phase 2: Transition to broadcast stream, skipping events at or before snap_ts.
    loop {
        match broadcast_rx.recv().await {
            Ok(event) => {
                let ts = match &event {
                    ChangeEvent::Upsert { timestamp, .. } => *timestamp,
                    ChangeEvent::Delete { timestamp, .. } => *timestamp,
                    ChangeEvent::Progress { timestamp, .. } => *timestamp,
                };
                if ts <= snap_ts {
                    continue;
                }
                let event_type = match &event {
                    ChangeEvent::Upsert { .. } => "upsert",
                    ChangeEvent::Delete { .. } => "delete",
                    ChangeEvent::Progress { .. } => "progress",
                };
                let sse = Event::default()
                    .event(event_type)
                    .json_data(&event)
                    .unwrap();
                if tx.send(Ok(sse)).await.is_err() {
                    return;
                }
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                tracing::warn!(skipped = n, view = %view_name, "snapshot broadcast lagged");
                let warn = Event::default()
                    .event("error")
                    .data(
                        serde_json::json!({"type": "lagged", "skipped": n}).to_string(),
                    );
                let _ = tx.send(Ok(warn)).await;
                return;
            }
            Err(broadcast::error::RecvError::Closed) => return,
        }
    }
}

// ---------------------------------------------------------------------------
// WebSocket — passthrough of the SUBSCRIBE stream
// ---------------------------------------------------------------------------

/// Upgrade to WebSocket. Optionally send snapshot first via `?snapshot=true`.
/// Each message is a JSON-serialized ChangeEvent.
/// WebSocket stream. Same catchup modes as SSE:
///   ?snapshot=true     — full snapshot then live
///   ?since=<timestamp> — replay changelog, snapshot from Materialize if compacted
///   (neither)          — live only
async fn view_ws(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    ws: WebSocketUpgrade,
) -> Result<impl IntoResponse, StatusCode> {
    let vs = state.store.get_view(&name).ok_or(StatusCode::NOT_FOUND)?;
    let conn = state.connections.get(&name).cloned();
    Ok(ws.on_upgrade(move |socket| handle_ws(socket, vs, name, params, conn)))
}

async fn handle_ws(
    mut socket: WebSocket,
    vs: Arc<ViewStore>,
    view_name: String,
    params: HashMap<String, String>,
    conn: Option<ResolvedConnection>,
) {
    let since: Option<MzTimestamp> = params.get("since").and_then(|s| s.parse().ok());
    let compacted = since
        .map(|ts| vs.changes_since(ts).is_none())
        .unwrap_or(false);

    // Subscribe BEFORE building catchup to avoid gaps.
    let mut rx = vs.tx.subscribe();

    // skip_before: broadcast events at or before this timestamp are filtered out
    // (they're already covered by the Mz snapshot).
    let skip_before: MzTimestamp = if compacted {
        if let Some(conn) = conn {
            match fetch_mz_snapshot(&conn, &view_name).await {
                Ok(snapshot) => {
                    let snap_ts = snapshot.timestamp;
                    let key_count = snapshot.key_count;
                    for (key, value) in snapshot.keys {
                        let msg = serde_json::to_string(&ChangeEvent::Upsert {
                            view: view_name.clone(),
                            key,
                            value,
                            timestamp: snap_ts,
                        })
                        .unwrap_or_default();
                        if socket.send(Message::Text(msg.into())).await.is_err() {
                            return;
                        }
                    }
                    let complete = serde_json::json!({
                        "type": "catchup_complete",
                        "mode": "snapshot",
                        "keys": key_count,
                        "progress": snap_ts,
                        "source": "materialize"
                    });
                    if socket
                        .send(Message::Text(complete.to_string().into()))
                        .await
                        .is_err()
                    {
                        return;
                    }
                    snap_ts
                }
                Err(e) => {
                    tracing::error!(error = %e, view = %view_name, "client Mz snapshot failed");
                    let err = serde_json::json!({"type": "error", "message": "snapshot failed"});
                    let _ = socket.send(Message::Text(err.to_string().into())).await;
                    return;
                }
            }
        } else {
            tracing::error!(view = %view_name, "no connection config for Mz snapshot");
            return;
        }
    } else {
        // Non-compacted: send catchup from local changelog or snapshot.
        let catchup = build_ws_catchup(&vs, &view_name, &params);
        for msg in catchup {
            if socket.send(Message::Text(msg.into())).await.is_err() {
                return;
            }
        }
        0
    };

    // Live stream — forward broadcast events, skipping any at or before skip_before.
    loop {
        tokio::select! {
            result = rx.recv() => {
                match result {
                    Ok(event) => {
                        if skip_before > 0 {
                            let ts = match &event {
                                ChangeEvent::Upsert { timestamp, .. } => *timestamp,
                                ChangeEvent::Delete { timestamp, .. } => *timestamp,
                                ChangeEvent::Progress { timestamp, .. } => *timestamp,
                            };
                            if ts <= skip_before { continue; }
                        }
                        let json = serde_json::to_string(&event).unwrap_or_default();
                        if socket.send(Message::Text(json.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        let warn = serde_json::json!({"type": "lagged", "skipped": n});
                        let _ = socket.send(Message::Text(warn.to_string().into())).await;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
            // Also listen for client messages (ping/pong/close).
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(Message::Ping(data))) => {
                        if socket.send(Message::Pong(data)).await.is_err() { break; }
                    }
                    _ => {} // ignore text/binary from client
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Webhook management
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct RegisterWebhookRequest {
    url: String,
    secret: Option<String>,
    #[serde(default)]
    views: Vec<String>,
}

async fn list_webhooks(State(state): State<AppState>) -> Json<Vec<WebhookEntry>> {
    let hooks = state.webhooks.read().await;
    Json(hooks.clone())
}

async fn register_webhook(
    State(state): State<AppState>,
    Json(req): Json<RegisterWebhookRequest>,
) -> Result<(StatusCode, Json<WebhookEntry>), StatusCode> {
    let entry = WebhookEntry {
        id: uuid::Uuid::new_v4().to_string(),
        url: req.url,
        secret: req.secret,
        views: req.views,
    };
    let mut hooks = state.webhooks.write().await;
    hooks.push(entry.clone());
    Ok((StatusCode::CREATED, Json(entry)))
}

async fn unregister_webhook(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> StatusCode {
    let mut hooks = state.webhooks.write().await;
    let before = hooks.len();
    hooks.retain(|h| h.id != id);
    if hooks.len() < before {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    }
}

// ---------------------------------------------------------------------------
// Webhook test echo endpoint
// ---------------------------------------------------------------------------

async fn receive_webhook_test(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> StatusCode {
    let sig = headers
        .get("X-Webhook-Signature")
        .and_then(|v| v.to_str().ok())
        .map(String::from);

    let payload = WebhookTestPayload {
        received_at: chrono::Utc::now().to_rfc3339(),
        signature: sig,
        body,
    };

    let mut log = state.webhook_test_log.write().await;
    if log.len() >= 100 {
        log.drain(0..50); // keep last 50
    }
    log.push(payload);
    StatusCode::OK
}

async fn list_webhook_test(State(state): State<AppState>) -> Json<Vec<WebhookTestPayload>> {
    let log = state.webhook_test_log.read().await;
    Json(log.clone())
}

async fn clear_webhook_test(State(state): State<AppState>) -> StatusCode {
    let mut log = state.webhook_test_log.write().await;
    log.clear();
    StatusCode::NO_CONTENT
}

// ---------------------------------------------------------------------------
// Webhook dispatcher — rolls up changes between progress events
// ---------------------------------------------------------------------------

/// The final state of a key within a progress window.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum KeyChange {
    Upsert { key: String, value: String },
    Delete { key: String },
}

/// Payload sent to webhooks on each progress boundary.
#[derive(Debug, Clone, Serialize)]
struct WebhookBatch {
    view: String,
    timestamp: u64,
    changes: Vec<KeyChange>,
}

pub fn spawn_webhook_dispatcher(state: AppState) {
    let store = state.store.clone();
    let webhooks = state.webhooks.clone();
    let http = reqwest::Client::new();

    for entry in store.views.iter() {
        let vs = entry.value().clone();
        let webhooks = webhooks.clone();
        let http = http.clone();

        tokio::spawn(async move {
            let mut rx = vs.tx.subscribe();
            // Accumulate per-key final state between progress events.
            // IndexMap preserves insertion order for deterministic output.
            let mut pending: std::collections::HashMap<String, KeyChange> =
                std::collections::HashMap::new();

            loop {
                match rx.recv().await {
                    Ok(ChangeEvent::Upsert { key, value, .. }) => {
                        pending.insert(
                            key.clone(),
                            KeyChange::Upsert { key, value },
                        );
                    }
                    Ok(ChangeEvent::Delete { key, .. }) => {
                        pending.insert(
                            key.clone(),
                            KeyChange::Delete { key },
                        );
                    }
                    Ok(ChangeEvent::Progress { timestamp, .. }) => {
                        if pending.is_empty() {
                            continue;
                        }
                        let changes: Vec<KeyChange> = pending.drain().map(|(_, v)| v).collect();
                        let batch = WebhookBatch {
                            view: vs.name.clone(),
                            timestamp,
                            changes,
                        };
                        let hooks = webhooks.read().await;
                        for hook in hooks.iter() {
                            if !hook.views.is_empty() && !hook.views.contains(&vs.name) {
                                continue;
                            }
                            let body_str =
                                serde_json::to_string(&batch).unwrap_or_default();
                            let mut req = http
                                .post(&hook.url)
                                .header("Content-Type", "application/json")
                                .body(body_str.clone());
                            if let Some(secret) = &hook.secret {
                                use hmac::{Hmac, Mac};
                                use sha2::Sha256;
                                type HmacSha256 = Hmac<Sha256>;
                                let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
                                    .expect("HMAC key");
                                mac.update(body_str.as_bytes());
                                let sig = hex::encode(mac.finalize().into_bytes());
                                req = req.header("X-Webhook-Signature", sig);
                            }
                            tokio::spawn(async move {
                                if let Err(e) = req.send().await {
                                    tracing::warn!(error = %e, "webhook delivery failed");
                                }
                            });
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(skipped = n, view = %vs.name, "webhook receiver lagged");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    }
}
