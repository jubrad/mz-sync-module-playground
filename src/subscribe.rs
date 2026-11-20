use materialize_sdk::core::Envelope;
use materialize_sdk::{Change, Event, LiveOptions, LiveSubscription, Row, Subscribe, Update};
use tokio_postgres::tls::MakeTlsConnect;
use tokio_postgres::{SimpleQueryMessage, Socket};
use tracing::info;

use crate::config::ResolvedConnection;
use crate::error::Error;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// A change to one key of a view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ViewChange {
    Upsert { key: String, value: String },
    Delete { key: String },
}

// ---------------------------------------------------------------------------
// SQL helpers
// ---------------------------------------------------------------------------

use crate::store::ColumnInfo;

/// Probe the user's SQL to discover column names and types.
pub async fn probe_columns(
    client: &tokio_postgres::Client,
    sql: &str,
) -> Result<Vec<ColumnInfo>, Error> {
    let stmt = client
        .prepare(&format!(
            "SELECT * FROM ({sql}) AS _probe WHERE false LIMIT 0"
        ))
        .await?;
    let cols: Vec<ColumnInfo> = stmt.columns().iter().map(|c| ColumnInfo {
        name: c.name().to_string(),
        pg_type: c.type_().name().to_string(),
    }).collect();
    if cols.is_empty() {
        return Err(Error::Validation("query returns no columns".into()));
    }
    Ok(cols)
}

/// Pick the key column: explicit config > column named "id" > first column.
fn pick_key_column<'a>(columns: &'a [String], config_key: Option<&'a str>) -> Result<&'a str, Error> {
    if let Some(k) = config_key {
        if columns.iter().any(|c| c == k) {
            return Ok(k);
        }
        return Err(Error::Validation(format!(
            "configured key_column '{}' not found in columns: {:?}",
            k, columns
        )));
    }
    // Auto-detect: prefer "id", then "key", then first column.
    for candidate in &["id", "key"] {
        if columns.iter().any(|c| c.as_str() == *candidate) {
            return Ok(candidate);
        }
    }
    Ok(&columns[0])
}

/// If the query already returns exactly `key` and `value`, use it as-is.
/// Otherwise, wrap it to produce key/value from arbitrary columns.
fn resolve_sql(original: &str, columns: &[String], key_col: &str) -> String {
    // Already has key/value — pass through.
    if columns.len() == 2 && columns.contains(&"key".to_string()) && columns.contains(&"value".to_string()) {
        return original.to_string();
    }

    let value_cols: Vec<&String> = columns.iter().filter(|c| c.as_str() != key_col).collect();
    let pairs: String = value_cols
        .iter()
        .map(|c| format!("'{}', \"{}\"", c, c))
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        "SELECT \"{}\"::text AS key, jsonb_build_object({})::text AS value FROM ({}) AS _mz_http_src",
        key_col, pairs, original
    )
}

// ---------------------------------------------------------------------------
// Decoding
// ---------------------------------------------------------------------------

/// The single cell of a one-column key or value.
fn cell(row: Row, what: &str) -> Result<String, Error> {
    row.into_iter()
        .next()
        .flatten()
        .ok_or_else(|| Error::Subscribe(format!("{what} is NULL")))
}

/// Turns an update of the `key`/`value` subscription into a [`ViewChange`].
pub fn key_change(update: Update) -> Result<ViewChange, Error> {
    match update.change {
        Change::Upsert { key, value } => Ok(ViewChange::Upsert {
            key: cell(key, "key")?,
            value: cell(value, "value")?,
        }),
        Change::Delete { key } => Ok(ViewChange::Delete {
            key: cell(key, "key")?,
        }),
        Change::KeyViolation { key } => Err(Error::Subscribe(format!(
            "key {:?} is not unique in the view; set key_column to a unique column",
            key.into_iter().next().flatten()
        ))),
        Change::Diff { .. } => Err(Error::Subscribe(
            "got a diff update from an upsert subscription".into(),
        )),
    }
}

// ---------------------------------------------------------------------------
// Connection settings
// ---------------------------------------------------------------------------

/// Escapes a value for the `options` connection parameter, where a space
/// separates settings and a backslash escapes the next character.
fn escape_option(value: &str) -> String {
    value.replace('\\', "\\\\").replace(' ', "\\ ")
}

/// The connection settings for a view.
///
/// The SDK's subscription owns its session, so session settings (cluster,
/// schema, and any session variables) travel in the `options` parameter
/// instead of `SET` statements.
fn pg_config(conn: &ResolvedConnection) -> tokio_postgres::Config {
    let mut options = vec!["--welcome_message=off".to_string()];
    if let Some(cluster) = &conn.cluster {
        options.push(format!("--cluster={}", escape_option(cluster)));
    }
    if let Some(schema) = &conn.schema {
        options.push(format!("--search_path={}", escape_option(schema)));
    }
    let mut variables: Vec<_> = conn.session_variables.iter().collect();
    variables.sort();
    for (name, value) in variables {
        options.push(format!("--{name}={}", escape_option(value)));
    }

    let mut config = tokio_postgres::Config::new();
    config
        .host(&conn.host)
        .port(conn.port)
        .user(&conn.user)
        .password(&conn.password)
        .dbname(&conn.database)
        .application_name("mz-http")
        .options(&options.join(" "));
    config
}

// ---------------------------------------------------------------------------
// MzSubscriber
// ---------------------------------------------------------------------------

/// A live subscription to one view, run by the Materialize SDK.
///
/// The SDK owns the cursor: it fetches, checks the protocol, and releases
/// changes as batches at closed frontiers. A view is a query, so the
/// subscription always starts with a snapshot and cannot resume.
pub struct MzSubscriber {
    live: LiveSubscription,
}

impl MzSubscriber {
    pub async fn connect(
        conn: &ResolvedConnection,
    ) -> Result<(Self, Vec<ColumnInfo>, String), Error> {
        let config = pg_config(conn);
        if conn.tls {
            let tls = postgres_native_tls::MakeTlsConnector::new(native_tls::TlsConnector::new()?);
            Self::connect_with(conn, &config, tls).await
        } else {
            Self::connect_with(conn, &config, tokio_postgres::NoTls).await
        }
    }

    async fn connect_with<T>(
        conn: &ResolvedConnection,
        config: &tokio_postgres::Config,
        tls: T,
    ) -> Result<(Self, Vec<ColumnInfo>, String), Error>
    where
        T: MakeTlsConnect<Socket> + Clone,
        T::Stream: Send + 'static,
    {
        // Probe the view's columns on a short-lived session of its own.
        let (column_info, key_col, effective_sql) = {
            let (client, connection) = config.connect(tls.clone()).await?;
            let handle = tokio::spawn(connection);
            let rows = client
                .simple_query(
                    "SELECT mz_environment_id(), current_database(), current_schema(), current_role(), mz_catalog.mz_version()",
                )
                .await?;
            for msg in &rows {
                if let SimpleQueryMessage::Row(row) = msg {
                    info!(
                        env = row.get(0).unwrap_or("?"),
                        db = row.get(1).unwrap_or("?"),
                        schema = row.get(2).unwrap_or("?"),
                        role = row.get(3).unwrap_or("?"),
                        version = row.get(4).unwrap_or("?"),
                        "connected to Materialize"
                    );
                }
            }
            let sql = conn.sql.trim().trim_end_matches(';').trim_end();
            let column_info = probe_columns(&client, sql).await?;
            let col_names: Vec<String> = column_info.iter().map(|c| c.name.clone()).collect();
            let key_col = pick_key_column(&col_names, conn.key_column.as_deref())?.to_string();
            let effective_sql = resolve_sql(sql, &col_names, &key_col);
            info!(
                columns = ?col_names,
                key_column = %key_col,
                wrapped = (effective_sql != sql),
                "resolved view schema"
            );
            drop(client);
            let _ = handle.await;
            (column_info, key_col, effective_sql)
        };

        let subscribe = Subscribe::query(effective_sql).with_envelope(Envelope::Upsert {
            key: vec!["key".into()],
        });
        let options = LiveOptions::default().max_rows(u64::from(conn.fetch_size));
        info!("starting subscribe");
        let live = LiveSubscription::connect(config, tls, subscribe, options).await?;

        Ok((Self { live }, column_info, key_col))
    }

    /// The next event, or `None` once the stream has ended.
    pub async fn next(&mut self) -> Result<Option<Event>, Error> {
        self.live.next().await.transpose().map_err(Error::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_wraps_arbitrary_columns() {
        let cols = vec!["pr_id".into(), "repo".into(), "author".into()];
        let sql = resolve_sql("SELECT * FROM prs", &cols, "pr_id");
        assert!(sql.contains("AS key"));
        assert!(sql.contains("jsonb_build_object"));
        assert!(sql.contains("'repo'"));
        assert!(sql.contains("'author'"));
        assert!(!sql.contains("'pr_id', \"pr_id\"")); // key excluded from value
    }

    #[test]
    fn passthrough_key_value() {
        let cols = vec!["key".into(), "value".into()];
        let original = "SELECT k AS key, v AS value FROM t";
        let sql = resolve_sql(original, &cols, "key");
        assert_eq!(sql, original);
    }

    #[test]
    fn pick_key_prefers_id() {
        let cols = vec!["name".into(), "id".into(), "status".into()];
        assert_eq!(pick_key_column(&cols, None).unwrap(), "id");
    }

    #[test]
    fn pick_key_falls_back_to_first() {
        let cols = vec!["repo".into(), "author".into()];
        assert_eq!(pick_key_column(&cols, None).unwrap(), "repo");
    }

    #[test]
    fn pick_key_respects_config() {
        let cols = vec!["repo".into(), "author".into()];
        assert_eq!(pick_key_column(&cols, Some("author")).unwrap(), "author");
    }

    #[test]
    fn pick_key_config_not_found() {
        let cols = vec!["repo".into(), "author".into()];
        assert!(pick_key_column(&cols, Some("missing")).is_err());
    }

    #[test]
    fn options_carry_session_settings() {
        let mut conn = crate::config::ResolvedConnection {
            host: "h".into(),
            port: 6875,
            user: "u".into(),
            password: "p".into(),
            database: "materialize".into(),
            cluster: Some("serving".into()),
            schema: Some("my schema".into()),
            tls: false,
            fetch_size: 100,
            key_column: None,
            title_columns: vec![],
            session_variables: Default::default(),
            sql: "SELECT 1".into(),
        };
        conn.session_variables
            .insert("statement_timeout".into(), "5s".into());
        let config = pg_config(&conn);
        assert_eq!(
            config.get_options(),
            Some("--welcome_message=off --cluster=serving --search_path=my\\ schema --statement_timeout=5s")
        );
    }

    #[test]
    fn key_change_decodes_upserts_and_deletes() {
        let upsert = Update {
            timestamp: 1,
            change: Change::Upsert {
                key: vec![Some("a".into())],
                value: vec![Some("{}".into())],
            },
        };
        assert_eq!(
            key_change(upsert).unwrap(),
            ViewChange::Upsert { key: "a".into(), value: "{}".into() }
        );
        let delete = Update {
            timestamp: 1,
            change: Change::Delete { key: vec![Some("a".into())] },
        };
        assert_eq!(key_change(delete).unwrap(), ViewChange::Delete { key: "a".into() });
        let violation = Update {
            timestamp: 1,
            change: Change::KeyViolation { key: vec![Some("a".into())] },
        };
        assert!(key_change(violation).is_err());
    }
}
