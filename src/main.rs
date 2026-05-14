mod config;
mod error;
mod pipeline;
mod server;
mod store;
mod subscribe;

use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use tokio::sync::{watch, RwLock};
use tracing::info;

use config::Config;
use server::{AppState, WebhookEntry};
use store::Store;

#[derive(Parser)]
#[command(name = "mz-http", about = "Materialize HTTP serving layer")]
struct Cli {
    /// Path to the YAML configuration file.
    #[arg(short, long, default_value = "config.yaml")]
    config: String,

    /// Directory to serve static files from (e.g. ./static).
    #[arg(short, long)]
    static_dir: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "mz_http=info".into()),
        )
        .init();

    let cli = Cli::parse();
    let config = Config::load(&cli.config)?;

    info!(
        views = config.views.len(),
        bind = %config.server.bind,
        "loaded configuration"
    );

    let store = Store::new();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Register all views and spawn subscribe pipelines.
    let mut handles = Vec::new();
    let mut connections = std::collections::HashMap::new();
    for view_cfg in &config.views {
        let resolved = view_cfg.resolve(&config.defaults);
        connections.insert(view_cfg.name.clone(), resolved.clone());
        let view_store = store.register_view(&view_cfg.name, resolved.title_columns.clone());
        let name = view_cfg.name.clone();
        let rx = shutdown_rx.clone();

        let handle = tokio::spawn(async move {
            pipeline::run_view(name, resolved, view_store, rx).await;
        });
        handles.push(handle);
    }

    // Seed webhook registry from config.
    let initial_hooks: Vec<WebhookEntry> = config
        .webhooks
        .iter()
        .map(|wh| WebhookEntry {
            id: uuid::Uuid::new_v4().to_string(),
            url: wh.url.clone(),
            secret: wh.secret.clone(),
            views: wh.views.clone(),
        })
        .collect();

    let app_state = AppState {
        store: store.clone(),
        connections: Arc::new(connections),
        webhooks: Arc::new(RwLock::new(initial_hooks)),
        webhook_test_log: Arc::new(RwLock::new(Vec::new())),
    };

    // Start webhook dispatcher.
    server::spawn_webhook_dispatcher(app_state.clone());

    // Start HTTP server.
    let router = server::build_router(app_state, cli.static_dir);
    let listener = tokio::net::TcpListener::bind(&config.server.bind).await?;
    info!(bind = %config.server.bind, "HTTP server listening");

    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            tokio::signal::ctrl_c()
                .await
                .expect("failed to listen for ctrl-c");
            info!("ctrl-c received; shutting down");
            let _ = shutdown_tx.send(true);
        })
        .await?;

    // Give pipelines a moment to notice shutdown, then abort stragglers.
    // SUBSCRIBE FETCH can block indefinitely so we can't wait forever.
    tokio::time::sleep(Duration::from_secs(1)).await;
    for handle in handles {
        handle.abort();
    }

    info!("clean shutdown complete");
    Ok(())
}
