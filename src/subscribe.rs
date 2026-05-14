use tokio_postgres::SimpleQueryMessage;
use tracing::{debug, info};

use crate::config::ResolvedConnection;
use crate::error::{Error, MzTimestamp};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubscribeEvent {
    Upsert { key: String, value: String },
    Delete { key: String },
    Progress(MzTimestamp),
}

// ---------------------------------------------------------------------------
// SQL helpers
// ---------------------------------------------------------------------------

/// Build the DECLARE CURSOR for SUBSCRIBE with ENVELOPE UPSERT.
/// `key_col` is the column name used as the upsert key.
fn build_subscribe_sql(sql: &str, key_col: &str, as_of: Option<MzTimestamp>) -> String {
    let quoted_key = format!("\"{}\"", key_col);
    match as_of {
        Some(ts) => format!(
            "DECLARE c CURSOR FOR SUBSCRIBE ({sql}) WITH (PROGRESS) AS OF {ts} ENVELOPE UPSERT (KEY ({quoted_key}))"
        ),
        None => format!(
            "DECLARE c CURSOR FOR SUBSCRIBE ({sql}) WITH (SNAPSHOT, PROGRESS) ENVELOPE UPSERT (KEY ({quoted_key}))"
        ),
    }
}

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
// Row parsing
// ---------------------------------------------------------------------------

pub fn parse_subscribe_row(row: &tokio_postgres::SimpleQueryRow) -> Result<SubscribeEvent, Error> {
    let mz_timestamp: MzTimestamp = row
        .get(0)
        .ok_or_else(|| Error::Subscribe("missing mz_timestamp".into()))?
        .parse()
        .map_err(|e| Error::Subscribe(format!("invalid mz_timestamp: {e}")))?;

    let mz_progressed = row
        .get(1)
        .ok_or_else(|| Error::Subscribe("missing mz_progressed".into()))?;

    if mz_progressed == "t" {
        return Ok(SubscribeEvent::Progress(mz_timestamp));
    }

    let mz_state = row
        .get(2)
        .ok_or_else(|| Error::Subscribe("missing mz_state for data row".into()))?;

    let key = row
        .get(3)
        .ok_or_else(|| Error::Subscribe("missing key for data row".into()))?
        .to_string();

    match mz_state {
        "upsert" => {
            let value = row
                .get(4)
                .ok_or_else(|| Error::Subscribe("missing value for upsert".into()))?
                .to_string();
            Ok(SubscribeEvent::Upsert { key, value })
        }
        "delete" => Ok(SubscribeEvent::Delete { key }),
        other => Err(Error::Subscribe(format!("unknown mz_state: {other}"))),
    }
}

// ---------------------------------------------------------------------------
// MzSubscriber
// ---------------------------------------------------------------------------

pub struct MzSubscriber {
    client: tokio_postgres::Client,
    fetch_size: u32,
    _connection_handle: tokio::task::JoinHandle<()>,
}

impl MzSubscriber {
    pub async fn connect(
        conn: &ResolvedConnection,
        as_of: Option<MzTimestamp>,
    ) -> Result<(Self, Vec<ColumnInfo>, String), Error> {
        let connstr = format!(
            "host={} port={} user={} password={} dbname={} application_name=mz-http options=--welcome_message=off",
            conn.host, conn.port, conn.user, conn.password, conn.database
        );

        let (client, handle) = if conn.tls {
            let tls_connector = native_tls::TlsConnector::new()?;
            let connector = postgres_native_tls::MakeTlsConnector::new(tls_connector);
            let (client, connection) = tokio_postgres::connect(&connstr, connector).await?;
            let handle = tokio::spawn(async move {
                if let Err(e) = connection.await {
                    tracing::error!(error = %e, "materialize connection lost");
                }
            });
            (client, handle)
        } else {
            let (client, connection) =
                tokio_postgres::connect(&connstr, tokio_postgres::NoTls).await?;
            let handle = tokio::spawn(async move {
                if let Err(e) = connection.await {
                    tracing::error!(error = %e, "materialize connection lost");
                }
            });
            (client, handle)
        };

        // Log connection metadata.
        let rows = client
            .simple_query(
                "SELECT mz_environment_id(), current_database(), current_schema(), current_role()",
            )
            .await?;
        for msg in &rows {
            if let SimpleQueryMessage::Row(row) = msg {
                info!(
                    env = row.get(0).unwrap_or("?"),
                    db = row.get(1).unwrap_or("?"),
                    schema = row.get(2).unwrap_or("?"),
                    role = row.get(3).unwrap_or("?"),
                    "connected to Materialize"
                );
            }
        }

        // Optional cluster override.
        if let Some(cluster) = &conn.cluster {
            client
                .simple_query(&format!("SET CLUSTER = {cluster}"))
                .await?;
            info!(cluster = %cluster, "set cluster");
        }

        // Optional schema override.
        if let Some(schema) = &conn.schema {
            client
                .simple_query(&format!("SET search_path = '{schema}'"))
                .await?;
        }

        // Arbitrary session variables.
        for (k, v) in &conn.session_variables {
            client
                .simple_query(&format!("SET {k} = '{v}'"))
                .await?;
            debug!(var = %k, val = %v, "set session variable");
        }

        // Probe columns, pick key, resolve SQL.
        let column_info = probe_columns(&client, &conn.sql).await?;
        let col_names: Vec<String> = column_info.iter().map(|c| c.name.clone()).collect();
        let key_col = pick_key_column(&col_names, conn.key_column.as_deref())?;
        let effective_sql = resolve_sql(&conn.sql, &col_names, key_col);

        info!(
            columns = ?col_names,
            key_column = %key_col,
            wrapped = (effective_sql != conn.sql),
            "resolved view schema"
        );

        // Open the subscribe cursor.
        let subscribe_sql = build_subscribe_sql(&effective_sql, "key", as_of);
        info!(sql = %subscribe_sql, "starting subscribe");

        client.simple_query("BEGIN").await?;
        client.simple_query(&subscribe_sql).await?;

        Ok((
            Self {
                client,
                fetch_size: conn.fetch_size,
                _connection_handle: handle,
            },
            column_info,
            key_col.to_string(),
        ))
    }

    pub async fn recv(&mut self) -> Result<Vec<SubscribeEvent>, Error> {
        let fetch_sql = format!("FETCH {} c", self.fetch_size);
        let messages = self.client.simple_query(&fetch_sql).await.map_err(|e| {
            let msg = e.to_string();
            if msg.contains("is not valid for all inputs") {
                let ts = msg
                    .split("Timestamp (")
                    .nth(1)
                    .and_then(|s| s.split(')').next())
                    .and_then(|s| s.parse::<MzTimestamp>().ok())
                    .unwrap_or(0);
                Error::TimestampCompacted(ts)
            } else {
                Error::Materialize(e)
            }
        })?;

        let mut events = Vec::new();
        for msg in messages {
            if let SimpleQueryMessage::Row(row) = msg {
                events.push(parse_subscribe_row(&row)?);
            }
        }

        debug!(count = events.len(), "fetched events");
        Ok(events)
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
    fn subscribe_sql_quotes_key() {
        let sql = build_subscribe_sql("SELECT key, value FROM mv", "key", None);
        assert!(sql.contains("KEY (\"key\")"));
        assert!(sql.contains("SNAPSHOT"));
    }
}
