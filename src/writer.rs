use anyhow::{Context, Result};
use futures_util::SinkExt;
use polars::prelude::*;
use tokio_postgres::Client;
use tracing::{debug, info};

/// Serialize a DataFrame to CSV bytes (no header).
fn dataframe_to_csv_bytes(df: &mut DataFrame) -> Result<Vec<u8>> {
    let mut buf: Vec<u8> = Vec::new();
    CsvWriter::new(&mut buf)
        .include_header(false)
        .finish(df)
        .context("Failed to serialize DataFrame to CSV")?;
    Ok(buf)
}

/// Write an entire DataFrame to PostgreSQL using COPY FROM STDIN WITH CSV.
///
/// This "buffered" approach serializes the full DataFrame to an in-memory CSV
/// buffer and then streams it to PostgreSQL in one shot.
pub async fn write_dataframe_to_postgres(
    df: &mut DataFrame,
    client: &Client,
    table: &str,
) -> Result<u64> {
    info!(
        rows = df.height(),
        table = table,
        "Starting buffered COPY ingestion"
    );

    // Serialize entire DataFrame to CSV (no header)
    let csv_bytes = dataframe_to_csv_bytes(df)?;
    let total_bytes = csv_bytes.len();

    info!(bytes = total_bytes, "CSV buffer ready, starting COPY");

    // Initiate COPY
    let copy_query = format!(
        "COPY {} (id, name, value, category, timestamp) FROM STDIN WITH (FORMAT csv)",
        table
    );

    let sink = client
        .copy_in::<_, bytes::Bytes>(&copy_query)
        .await
        .context("Failed to initiate COPY")?;

    futures_util::pin_mut!(sink);

    // Send CSV data in 64 KB write chunks
    let chunk_size = 64 * 1024;
    let mut offset = 0;

    while offset < csv_bytes.len() {
        let end = std::cmp::min(offset + chunk_size, csv_bytes.len());
        let chunk = bytes::Bytes::copy_from_slice(&csv_bytes[offset..end]);
        sink.send(chunk)
            .await
            .context("Failed to send data to COPY sink")?;
        offset = end;
    }

    let rows_inserted = sink.finish().await.context("Failed to finish COPY")?;

    info!(
        rows = rows_inserted,
        bytes = total_bytes,
        "Buffered COPY ingestion complete"
    );

    Ok(rows_inserted)
}

/// Write a DataFrame to PostgreSQL using COPY FROM STDIN WITH CSV in a
/// streaming, chunk-by-chunk fashion. This avoids building the entire CSV
/// in memory at once.
///
/// The DataFrame is sliced into chunks of `chunk_size` rows, each serialized
/// to CSV independently and streamed to the COPY sink.
pub async fn write_dataframe_streaming(
    df: &DataFrame,
    client: &Client,
    table: &str,
    chunk_size: usize,
) -> Result<u64> {
    let total_rows = df.height();
    let num_chunks = (total_rows + chunk_size - 1) / chunk_size;

    info!(
        rows = total_rows,
        chunk_size = chunk_size,
        num_chunks = num_chunks,
        table = table,
        "Starting streaming COPY ingestion"
    );

    let copy_query = format!(
        "COPY {} (id, name, value, category, timestamp) FROM STDIN WITH (FORMAT csv)",
        table
    );

    let sink = client
        .copy_in::<_, bytes::Bytes>(&copy_query)
        .await
        .context("Failed to initiate COPY")?;

    futures_util::pin_mut!(sink);

    let mut _rows_sent: u64 = 0;

    for chunk_idx in 0..num_chunks {
        let offset = chunk_idx * chunk_size;
        let length = std::cmp::min(chunk_size, total_rows - offset);

        let mut chunk_df = df.slice(offset as i64, length);

        // Serialize this chunk to CSV
        let csv_bytes = dataframe_to_csv_bytes(&mut chunk_df)?;
        let data = bytes::Bytes::from(csv_bytes);

        sink.send(data)
            .await
            .context("Failed to send chunk to COPY sink")?;

        _rows_sent += length as u64;

        debug!(
            chunk = chunk_idx + 1,
            total_chunks = num_chunks,
            rows_in_chunk = length,
            "Chunk sent"
        );
    }

    let rows_inserted = sink.finish().await.context("Failed to finish COPY")?;

    info!(
        rows = rows_inserted,
        "Streaming COPY ingestion complete"
    );

    Ok(rows_inserted)
}
