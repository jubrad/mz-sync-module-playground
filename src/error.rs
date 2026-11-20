use thiserror::Error;

/// Materialize logical timestamp.
pub type MzTimestamp = u64;

#[derive(Error, Debug)]
pub enum Error {
    #[error("configuration error: {0}")]
    Config(String),

    #[error("materialize error: {0}")]
    Materialize(#[from] tokio_postgres::Error),

    #[error("subscription error: {0}")]
    Sdk(#[from] materialize_sdk::Error),

    #[error("subscribe error: {0}")]
    Subscribe(String),

    #[error("TLS error: {0}")]
    Tls(#[from] native_tls::Error),

    #[error("validation error: {0}")]
    Validation(String),

}
