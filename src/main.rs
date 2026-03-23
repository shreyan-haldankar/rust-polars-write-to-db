mod benchmark;
mod config;
mod dataframe;
mod db;
mod utils;
mod writer;

use anyhow::Result;
use clap::{Parser, ValueEnum};
use tracing::info;

/// High-performance Polars DataFrame ingestion into PostgreSQL via COPY CSV.
#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Number of rows to generate
    #[arg(short, long, default_value_t = 1_000_000)]
    rows: usize,

    /// Ingestion mode
    #[arg(short, long, value_enum, default_value_t = Mode::Buffered)]
    mode: Mode,

    /// Chunk size for streaming mode (rows per chunk)
    #[arg(short, long, default_value_t = 50_000)]
    chunk_size: usize,
}

#[derive(Debug, Clone, ValueEnum)]
enum Mode {
    Buffered,
    Streaming,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // Load .env file
    dotenv::dotenv().ok();

    // Parse CLI arguments
    let args = Args::parse();

    info!(
        rows = args.rows,
        mode = ?args.mode,
        "Starting Polars → PostgreSQL COPY CSV ingestion"
    );

    // Load configuration
    let config = config::Config::from_env()?;
    info!(
        host = %config.db_host,
        port = config.db_port,
        db = %config.db_name,
        table = %config.table_name,
        "Configuration loaded"
    );

    // Connect to PostgreSQL
    let client = db::connect(&config).await?;

    // Create table if not exists
    db::create_table(&client, &config.table_name).await?;

    // Truncate for a clean benchmark
    db::truncate_table(&client, &config.table_name).await?;

    // Generate DataFrame
    info!(rows = args.rows, "Generating DataFrame");
    let mut df = dataframe::generate_dataframe(args.rows)?;
    info!(
        rows = df.height(),
        cols = df.width(),
        "DataFrame ready"
    );

    // Capture memory before ingestion
    let mem_before = benchmark::capture_memory();

    // Start timer
    let timer = benchmark::Timer::start();

    // Run ingestion
    let rows_inserted = match args.mode {
        Mode::Buffered => {
            writer::write_dataframe_to_postgres(&mut df, &client, &config.table_name).await?
        }
        Mode::Streaming => {
            writer::write_dataframe_streaming(&df, &client, &config.table_name, args.chunk_size)
                .await?
        }
    };

    // Stop timer
    let duration = timer.elapsed();

    // Capture memory after ingestion
    let mem_after = benchmark::capture_memory();

    // Build and print benchmark results
    let mode_label = match args.mode {
        Mode::Buffered => "buffered",
        Mode::Streaming => "streaming",
    };

    let result = benchmark::build_result(rows_inserted, duration, mem_before, mem_after, mode_label);
    result.print_report();

    info!("Done");
    Ok(())
}
