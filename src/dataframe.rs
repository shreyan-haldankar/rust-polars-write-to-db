use anyhow::Result;
use chrono::Utc;
use polars::prelude::*;
use rand::Rng;
use tracing::info;

/// Generate a Polars DataFrame with mock data.
///
/// Columns:
/// - id: i32 (sequential)
/// - name: Utf8 (random names)
/// - value: f64 (random floats)
/// - category: Utf8 (random categories)
/// - timestamp: Utf8 (current timestamp with slight variation)
pub fn generate_dataframe(n_rows: usize) -> Result<DataFrame> {
    info!(rows = n_rows, "Generating DataFrame");

    let mut rng = rand::thread_rng();

    let names = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Hank"];
    let categories = ["A", "B", "C", "D", "E"];

    let ids: Vec<i32> = (0..n_rows as i32).collect();

    let name_values: Vec<String> = (0..n_rows)
        .map(|_| {
            let idx = rng.gen_range(0..names.len());
            format!("{}_{}", names[idx], rng.gen_range(1000..9999))
        })
        .collect();

    let float_values: Vec<f64> = (0..n_rows)
        .map(|_| rng.gen_range(0.0..100_000.0))
        .collect();

    let category_values: Vec<String> = (0..n_rows)
        .map(|_| {
            let idx = rng.gen_range(0..categories.len());
            categories[idx].to_string()
        })
        .collect();

    let base_ts = Utc::now();
    let timestamp_values: Vec<String> = (0..n_rows)
        .map(|i| {
            let ts = base_ts + chrono::Duration::milliseconds(i as i64);
            ts.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
        })
        .collect();

    let df = DataFrame::new(vec![
        Series::new("id".into(), &ids).into(),
        Series::new("name".into(), &name_values).into(),
        Series::new("value".into(), &float_values).into(),
        Series::new("category".into(), &category_values).into(),
        Series::new("timestamp".into(), &timestamp_values).into(),
    ])?;

    info!(
        rows = df.height(),
        cols = df.width(),
        "DataFrame generated"
    );

    Ok(df)
}
