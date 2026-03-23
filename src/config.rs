use anyhow::{Context, Result};
use std::env;

/// Database and application configuration loaded from environment variables.
#[derive(Debug, Clone)]
pub struct Config {
    pub db_host: String,
    pub db_port: u16,
    pub db_user: String,
    pub db_password: String,
    pub db_name: String,
    pub table_name: String,
}

impl Config {
    /// Load configuration from environment variables.
    /// Expects: DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME, TABLE_NAME
    pub fn from_env() -> Result<Self> {
        let db_host = env::var("DB_HOST").context("DB_HOST not set")?;
        let db_port = env::var("DB_PORT")
            .context("DB_PORT not set")?
            .parse::<u16>()
            .context("DB_PORT must be a valid u16")?;
        let db_user = env::var("DB_USER").context("DB_USER not set")?;
        let db_password = env::var("DB_PASSWORD").context("DB_PASSWORD not set")?;
        let db_name = env::var("DB_NAME").context("DB_NAME not set")?;
        let table_name = env::var("TABLE_NAME").unwrap_or_else(|_| "polars_ingest_test".to_string());

        Ok(Self {
            db_host,
            db_port,
            db_user,
            db_password,
            db_name,
            table_name,
        })
    }

    /// Build a tokio-postgres connection string.
    pub fn connection_string(&self) -> String {
        format!(
            "host={} port={} user={} password={} dbname={}",
            self.db_host, self.db_port, self.db_user, self.db_password, self.db_name
        )
    }
}
