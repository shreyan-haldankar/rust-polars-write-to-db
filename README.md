# Rust Polars → PostgreSQL COPY CSV Ingestion

High-performance ingestion of [Polars](https://pola.rs/) DataFrames into PostgreSQL using the `COPY FROM STDIN WITH CSV` protocol.

## Architecture

```
┌──────────────┐     ┌──────────────┐     ┌───────────────────┐     ┌────────────┐
│  CLI (clap)  │────▶│  DataFrame   │────▶│  CSV Writer        │────▶│ PostgreSQL │
│  --rows      │     │  Generator   │     │  (buffered or      │     │ COPY FROM  │
│  --mode      │     │  (polars)    │     │   streaming)       │     │ STDIN CSV  │
└──────────────┘     └──────────────┘     └───────────────────┘     └────────────┘
                                                                          │
                                                                          ▼
                                                                   ┌────────────┐
                                                                   │ Benchmark  │
                                                                   │ Report     │
                                                                   └────────────┘
```

### Project Structure

```
.
├── Cargo.toml          # Dependencies and project metadata
├── .env                # Database configuration
├── .gitignore
├── README.md
└── src/
    ├── main.rs         # Entry point, CLI parsing, orchestration
    ├── config.rs       # Environment variable loading
    ├── db.rs           # PostgreSQL connection + table management
    ├── dataframe.rs    # Polars DataFrame generation with mock data
    ├── writer.rs       # Buffered & streaming COPY CSV writers
    ├── benchmark.rs    # Timing, throughput, memory metrics
    └── utils.rs        # Number/byte formatting helpers
```

## Features

- **Two ingestion modes**: buffered (full CSV in memory) and streaming (chunk-by-chunk)
- **COPY protocol**: uses PostgreSQL's fastest bulk-load path
- **Benchmarking**: measures time, throughput (rows/sec), and memory usage
- **Retry logic**: automatic retries on DB connection failure
- **Structured logging**: via `tracing`
- **CLI arguments**: configurable row count, mode, and chunk size

## Prerequisites

- **Rust** 1.75+ (2021 edition)
- **PostgreSQL** running locally (or accessible via network)

## Setup

1. **Clone the repository**

   ```bash
   git clone https://github.com/shreyan-haldankar/rust-polars-write-to-db.git
   cd rust-polars-write-to-db
   ```

2. **Configure the database**

   Copy and edit the `.env` file:

   ```bash
   cp .env.example .env
   ```

   Or create `.env` with:

   ```env
   DB_HOST=localhost
   DB_PORT=5432
   DB_USER=postgres
   DB_PASSWORD=postgres
   DB_NAME=test_db
   TABLE_NAME=polars_ingest_test
   ```

3. **Start PostgreSQL**

   **Option A: Docker (recommended)**

   ```bash
   docker run -d \
     --name postgres-local \
     -e POSTGRES_USER=postgres \
     -e POSTGRES_PASSWORD=postgres \
     -e POSTGRES_DB=test_db \
     -p 5432:5432 \
     postgres:16
   ```

   **Option B: Homebrew (macOS)**

   ```bash
   brew install postgresql@16
   brew services start postgresql@16
   createdb test_db
   ```

## Usage

```bash
# Default: 1,000,000 rows, buffered mode
cargo run --release

# Custom row count
cargo run --release -- --rows 500000

# Streaming mode with custom chunk size
cargo run --release -- --rows 2000000 --mode streaming --chunk-size 100000

# Enable debug logging
RUST_LOG=debug cargo run --release -- --rows 10000
```

### CLI Options

| Flag             | Description                          | Default     |
|------------------|--------------------------------------|-------------|
| `--rows`, `-r`   | Number of rows to generate           | 1,000,000   |
| `--mode`, `-m`   | Ingestion mode: `buffered`/`streaming` | `buffered`  |
| `--chunk-size`, `-c` | Rows per chunk (streaming mode)  | 50,000      |

## Example Output

```
==================================================
  BENCHMARK RESULTS (streaming)
==================================================
  Rows inserted:  2,000,000
  Time taken:     5.940 seconds
  Throughput:     336,672 rows/sec
  Memory before:  342.45 MB
  Memory after:   305.53 MB
  Memory delta:   -36.92 MB
==================================================
```

## How It Works

1. **Configuration**: Loads DB credentials from `.env` via `dotenv`
2. **Connection**: Establishes async PostgreSQL connection with retry logic (3 attempts)
3. **Table Setup**: Creates the target table if it doesn't exist, then truncates it
4. **DataFrame Generation**: Builds a Polars DataFrame with randomized mock data
5. **Ingestion**:
   - **Buffered**: Serializes the entire DataFrame to CSV in memory, then streams it to `COPY FROM STDIN`
   - **Streaming**: Slices the DataFrame into chunks, serializes each to CSV, and streams them individually
6. **Benchmark**: Reports rows inserted, wall-clock time, throughput, and memory delta

## Roadmap

Future ingestion strategies to explore as part of this POC:

- [ ] **Binary COPY (High-Performance)** — Use `COPY FROM STDIN WITH (FORMAT binary)` to skip CSV serialization overhead entirely. Send Polars columns as raw PostgreSQL binary tuples for maximum throughput.
- [ ] **ADBC Arrow Ingestion (Modern Stack)** — Leverage Arrow Database Connectivity (ADBC) to ingest Arrow-backed Polars DataFrames directly into PostgreSQL without intermediate CSV/binary conversion.
- [ ] **COPY via File (Disk-Based)** — Write the DataFrame to a CSV file on disk, then use `COPY FROM '/path/to/file'` for scenarios where memory is constrained or data needs to be auditable before ingestion.
- [ ] **DuckDB-Based Ingestion Architecture** — Use DuckDB as an intermediary: load the Polars DataFrame into DuckDB (zero-copy via Arrow), then use DuckDB's native `ATTACH` + `INSERT INTO` to push data to PostgreSQL.

## Benchmark Notes

- **Throughput** is calculated as `rows / elapsed_seconds`
- **Memory** is measured via `sysinfo` (process RSS before and after ingestion)
- For best results, use `--release` mode and a local PostgreSQL instance
- Streaming mode trades peak memory for slightly higher overhead per chunk

## License

MIT
