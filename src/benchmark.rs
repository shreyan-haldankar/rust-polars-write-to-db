use std::time::{Duration, Instant};
use sysinfo::System;
use tracing::info;

use crate::utils;

/// Snapshot of memory usage at a point in time.
#[derive(Debug, Clone, Copy)]
pub struct MemorySnapshot {
    pub used_bytes: u64,
}

/// Results of a benchmark run.
#[derive(Debug)]
pub struct BenchmarkResult {
    pub total_rows: u64,
    pub duration: Duration,
    pub rows_per_second: f64,
    pub memory_before: MemorySnapshot,
    pub memory_after: MemorySnapshot,
    pub mode: String,
}

impl BenchmarkResult {
    /// Pretty-print the benchmark results.
    pub fn print_report(&self) {
        let before = self.memory_before.used_bytes as i64;
        let after = self.memory_after.used_bytes as i64;
        let mem_diff = after - before;
        let mem_delta_str = if mem_diff >= 0 {
            format!("+{}", utils::format_bytes_mb(mem_diff as u64))
        } else {
            format!("-{}", utils::format_bytes_mb((-mem_diff) as u64))
        };

        println!("\n{}", "=".repeat(50));
        println!("  BENCHMARK RESULTS ({})", self.mode);
        println!("{}", "=".repeat(50));
        println!(
            "  Rows inserted:  {}",
            utils::format_number(self.total_rows)
        );
        println!("  Time taken:     {:.3} seconds", self.duration.as_secs_f64());
        println!(
            "  Throughput:     {} rows/sec",
            utils::format_number(self.rows_per_second as u64)
        );
        println!(
            "  Memory before:  {}",
            utils::format_bytes_mb(self.memory_before.used_bytes)
        );
        println!(
            "  Memory after:   {}",
            utils::format_bytes_mb(self.memory_after.used_bytes)
        );
        println!("  Memory delta:   {}", mem_delta_str);
        println!("{}\n", "=".repeat(50));
    }
}

/// Capture current process memory usage.
pub fn capture_memory() -> MemorySnapshot {
    let mut sys = System::new();
    sys.refresh_memory();

    let pid = sysinfo::get_current_pid().unwrap_or(sysinfo::Pid::from(0));
    sys.refresh_processes(
        sysinfo::ProcessesToUpdate::Some(&[pid]),
        true,
    );

    let used_bytes = sys
        .process(pid)
        .map(|p| p.memory())
        .unwrap_or(0);

    MemorySnapshot { used_bytes }
}

/// Timer utility for benchmarking.
pub struct Timer {
    start: Instant,
}

impl Timer {
    pub fn start() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}

/// Build a BenchmarkResult from collected data.
pub fn build_result(
    total_rows: u64,
    duration: Duration,
    memory_before: MemorySnapshot,
    memory_after: MemorySnapshot,
    mode: &str,
) -> BenchmarkResult {
    let rows_per_second = if duration.as_secs_f64() > 0.0 {
        total_rows as f64 / duration.as_secs_f64()
    } else {
        0.0
    };

    let result = BenchmarkResult {
        total_rows,
        duration,
        rows_per_second,
        memory_before,
        memory_after,
        mode: mode.to_string(),
    };

    info!(
        rows = total_rows,
        duration_secs = duration.as_secs_f64(),
        rows_per_sec = rows_per_second as u64,
        "Benchmark complete"
    );

    result
}
