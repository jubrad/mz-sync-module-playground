use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;

use crate::error::Error;

/// Top-level configuration.
#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub server: ServerConfig,
    pub defaults: ConnectionDefaults,
    pub views: Vec<ViewConfig>,
    #[serde(default)]
    pub webhooks: Vec<WebhookConfig>,
}

/// HTTP server settings.
#[derive(Debug, Deserialize, Clone)]
pub struct ServerConfig {
    #[serde(default = "default_bind")]
    pub bind: String,
}

fn default_bind() -> String {
    "0.0.0.0:8080".to_string()
}

/// Default Materialize connection fields — views inherit these.
#[derive(Debug, Deserialize, Clone)]
pub struct ConnectionDefaults {
    pub host: String,
    pub port: u16,
    pub user: String,
    #[serde(default)]
    pub password: String,
    pub database: String,
    #[serde(default)]
    pub tls: bool,
    #[serde(default = "default_fetch_size")]
    pub fetch_size: u32,
}

fn default_fetch_size() -> u32 {
    1024
}

/// A single view to subscribe to.
#[derive(Debug, Deserialize, Clone)]
pub struct ViewConfig {
    /// Unique name — used as the URL path segment.
    pub name: String,
    /// SQL query returning exactly `key` and `value` columns.
    pub sql: String,

    // Connection overrides (optional — fall back to defaults).
    pub host: Option<String>,
    pub port: Option<u16>,
    pub user: Option<String>,
    pub password: Option<String>,
    pub database: Option<String>,
    pub cluster: Option<String>,
    pub schema: Option<String>,
    pub tls: Option<bool>,
    pub fetch_size: Option<u32>,

    /// Which column to use as the key. If omitted, auto-detects:
    /// uses `key` column if present, otherwise the first column.
    #[serde(default)]
    pub key_column: Option<String>,

    /// Columns to show as a summary alongside the key in live streams.
    #[serde(default)]
    pub title_columns: Vec<String>,

    /// Arbitrary session variables set after connecting.
    #[serde(default)]
    pub session_variables: HashMap<String, String>,
}

/// Resolved connection config for a single view (defaults merged with overrides).
#[derive(Debug, Clone)]
pub struct ResolvedConnection {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    pub cluster: Option<String>,
    pub schema: Option<String>,
    pub tls: bool,
    pub fetch_size: u32,
    pub key_column: Option<String>,
    pub title_columns: Vec<String>,
    pub session_variables: HashMap<String, String>,
    pub sql: String,
}

impl ViewConfig {
    /// Merge this view's overrides with the top-level defaults.
    pub fn resolve(&self, defaults: &ConnectionDefaults) -> ResolvedConnection {
        ResolvedConnection {
            host: self.host.clone().unwrap_or_else(|| defaults.host.clone()),
            port: self.port.unwrap_or(defaults.port),
            user: self.user.clone().unwrap_or_else(|| defaults.user.clone()),
            password: self
                .password
                .clone()
                .unwrap_or_else(|| defaults.password.clone()),
            database: self
                .database
                .clone()
                .unwrap_or_else(|| defaults.database.clone()),
            cluster: self.cluster.clone(),
            schema: self.schema.clone(),
            tls: self.tls.unwrap_or(defaults.tls),
            fetch_size: self.fetch_size.unwrap_or(defaults.fetch_size),
            key_column: self.key_column.clone(),
            title_columns: self.title_columns.clone(),
            session_variables: self.session_variables.clone(),
            sql: self.sql.clone(),
        }
    }
}

/// Webhook registration from config file.
#[derive(Debug, Deserialize, Clone)]
pub struct WebhookConfig {
    pub url: String,
    #[serde(default)]
    pub secret: Option<String>,
    /// Which view names to forward. Empty means all views.
    #[serde(default)]
    pub views: Vec<String>,
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, Error> {
        let content = std::fs::read_to_string(path.as_ref())
            .map_err(|e| Error::Config(format!("failed to read config: {e}")))?;
        let expanded = expand_env_vars(&content);
        Self::from_str(&expanded)
    }

    pub fn from_str(yaml: &str) -> Result<Self, Error> {
        let config: Config = serde_yaml::from_str(yaml)
            .map_err(|e| Error::Config(format!("failed to parse config: {e}")))?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), Error> {
        if self.views.is_empty() {
            return Err(Error::Config("at least one view is required".into()));
        }

        let mut names = std::collections::HashSet::new();
        for v in &self.views {
            if v.name.is_empty() {
                return Err(Error::Config("view name cannot be empty".into()));
            }
            if v.sql.trim().is_empty() {
                return Err(Error::Config(format!(
                    "view '{}': sql cannot be empty",
                    v.name
                )));
            }
            if !names.insert(&v.name) {
                return Err(Error::Config(format!(
                    "duplicate view name: '{}'",
                    v.name
                )));
            }
        }

        if self.defaults.host.is_empty() {
            return Err(Error::Config("defaults.host cannot be empty".into()));
        }
        if self.defaults.port == 0 {
            return Err(Error::Config("defaults.port must be non-zero".into()));
        }
        if self.defaults.user.is_empty() {
            return Err(Error::Config("defaults.user cannot be empty".into()));
        }
        if self.defaults.database.is_empty() {
            return Err(Error::Config("defaults.database cannot be empty".into()));
        }
        if self.defaults.fetch_size == 0 {
            return Err(Error::Config(
                "defaults.fetch_size must be non-zero".into(),
            ));
        }

        // Validate webhook view references.
        for wh in &self.webhooks {
            if wh.url.is_empty() {
                return Err(Error::Config("webhook url cannot be empty".into()));
            }
            for vn in &wh.views {
                if !names.contains(vn) {
                    return Err(Error::Config(format!(
                        "webhook references unknown view: '{vn}'"
                    )));
                }
            }
        }

        Ok(())
    }
}

/// Expand `${VAR}` and `${VAR:-default}` patterns with environment variables.
/// Unset vars with no default are replaced with an empty string.
fn expand_env_vars(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '$' && chars.peek() == Some(&'{') {
            chars.next(); // consume '{'
            let mut var_expr = String::new();
            for ch in chars.by_ref() {
                if ch == '}' {
                    break;
                }
                var_expr.push(ch);
            }
            let (var_name, default) = if let Some(pos) = var_expr.find(":-") {
                (&var_expr[..pos], Some(&var_expr[pos + 2..]))
            } else {
                (var_expr.as_str(), None)
            };
            match std::env::var(var_name) {
                Ok(val) => result.push_str(&val),
                Err(_) => {
                    if let Some(def) = default {
                        result.push_str(def);
                    }
                }
            }
        } else {
            result.push(c);
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_minimal_config() {
        let yaml = r#"
server:
  bind: "0.0.0.0:9090"
defaults:
  host: localhost
  port: 6875
  user: mz
  database: mz
views:
  - name: test
    sql: "SELECT k AS key, v AS value FROM t"
"#;
        let cfg = Config::from_str(yaml).unwrap();
        assert_eq!(cfg.views.len(), 1);
        assert_eq!(cfg.server.bind, "0.0.0.0:9090");
    }

    #[test]
    fn resolve_uses_defaults() {
        let defaults = ConnectionDefaults {
            host: "h".into(),
            port: 6875,
            user: "u".into(),
            password: "p".into(),
            database: "d".into(),
            tls: true,
            fetch_size: 500,
        };
        let view = ViewConfig {
            name: "v".into(),
            sql: "SELECT 1".into(),
            host: None,
            port: None,
            user: None,
            password: None,
            database: Some("override_db".into()),
            cluster: Some("c1".into()),
            schema: None,
            tls: None,
            fetch_size: None,
            key_column: None,
            title_columns: vec![],
            session_variables: HashMap::new(),
        };
        let resolved = view.resolve(&defaults);
        assert_eq!(resolved.host, "h");
        assert_eq!(resolved.database, "override_db");
        assert_eq!(resolved.cluster.as_deref(), Some("c1"));
        assert!(resolved.tls);
        assert_eq!(resolved.fetch_size, 500);
    }

    #[test]
    fn duplicate_view_names_rejected() {
        let yaml = r#"
server:
  bind: "0.0.0.0:8080"
defaults:
  host: localhost
  port: 6875
  user: mz
  database: mz
views:
  - name: dup
    sql: "SELECT 1 AS key, 2 AS value"
  - name: dup
    sql: "SELECT 3 AS key, 4 AS value"
"#;
        let err = Config::from_str(yaml).unwrap_err();
        assert!(err.to_string().contains("duplicate"));
    }

    #[test]
    fn expand_env_vars_works() {
        std::env::set_var("MZ_TEST_HOST", "cloud.example.com");
        std::env::set_var("MZ_TEST_PASS", "secret123");

        assert_eq!(expand_env_vars("host: ${MZ_TEST_HOST}"), "host: cloud.example.com");
        assert_eq!(expand_env_vars("pw: ${MZ_TEST_PASS}"), "pw: secret123");
        assert_eq!(expand_env_vars("${UNSET_VAR_12345:-fallback}"), "fallback");
        assert_eq!(expand_env_vars("${UNSET_VAR_12345}"), "");
        assert_eq!(expand_env_vars("no vars here"), "no vars here");

        std::env::remove_var("MZ_TEST_HOST");
        std::env::remove_var("MZ_TEST_PASS");
    }
}
