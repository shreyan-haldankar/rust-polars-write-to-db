use anyhow::{Context, Result};
use tokio_postgres::{Client, NoTls};
use tracing::{info, warn};

use crate::config::Config;

const MAX_RETRIES: u32 = 3;
const RETRY_DELAY_MS: u64 = 1000;

/// Establish an async connection to PostgreSQL with retry logic.
pub async fn connect(config: &Config) -> Result<Client> {
    let conn_str = config.connection_string();

    for attempt in 1..=MAX_RETRIES {
        info!(attempt, max = MAX_RETRIES, "Connecting to PostgreSQL");

        match tokio_postgres::connect(&conn_str, NoTls).await {
            Ok((client, connection)) => {
                // Spawn the connection handler in the background
                tokio::spawn(async move {
                    if let Err(e) = connection.await {
                        tracing::error!(error = %e, "PostgreSQL connection error");
                    }
                });

                info!("Successfully connected to PostgreSQL");
                return Ok(client);
            }
            Err(e) => {
                warn!(
                    attempt,
                    error = %e,
                    "Failed to connect to PostgreSQL"
                );
                if attempt < MAX_RETRIES {
                    tokio::time::sleep(std::time::Duration::from_millis(RETRY_DELAY_MS)).await;
                } else {
                    return Err(e).context("Failed to connect after maximum retries");
                }
            }
        }
    }

    unreachable!()
}

/// Create the target table if it does not already exist.
pub async fn create_table(client: &Client, table_name: &str) -> Result<()> {
    let query = format!(
        r#"
        CREATE TABLE IF NOT EXISTS {} (
            id INT,
            name TEXT,
            value DOUBLE PRECISION,
            category TEXT,
            timestamp TEXT
        )
        "#,
        table_name
    );

    client
        .execute(&query, &[])
        .await
        .context("Failed to create table")?;

    info!(table = table_name, "Table ready");
    Ok(())
}

/// Truncate the target table to ensure a clean benchmark run.
pub async fn truncate_table(client: &Client, table_name: &str) -> Result<()> {
    let query = format!("TRUNCATE TABLE {}", table_name);

    client
        .execute(&query, &[])
        .await
        .context("Failed to truncate table")?;

    info!(table = table_name, "Table truncated");
    Ok(())
}
