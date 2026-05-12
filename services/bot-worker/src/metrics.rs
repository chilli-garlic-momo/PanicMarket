//! Real-time metrics collection for bot tests

use anyhow::Result;
use hdrhistogram::Histogram;
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tracing::info;
use uuid::Uuid;

pub struct TestMetrics {
    pub test_id: Uuid,
    start_time: Instant,
    total_sent: AtomicU64,
    total_success: AtomicU64,
    total_errors: AtomicU64,
    max_tps_observed: AtomicU64,
    // Latency histogram (nanoseconds)
    latency_hist: Mutex<Histogram<u64>>,
    // TPS tracking window
    window_count: AtomicU64,
    window_start: Mutex<Instant>,
}

impl TestMetrics {
    pub fn new(test_id: Uuid) -> Self {
        Self {
            test_id,
            start_time: Instant::now(),
            total_sent: AtomicU64::new(0),
            total_success: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
            max_tps_observed: AtomicU64::new(0),
            latency_hist: Mutex::new(
                Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3).unwrap()
            ),
            window_count: AtomicU64::new(0),
            window_start: Mutex::new(Instant::now()),
        }
    }

    pub fn record_sent(&self) {
        self.total_sent.fetch_add(1, Ordering::Relaxed);
        self.window_count.fetch_add(1, Ordering::Relaxed);

        // Check if 1s window elapsed, update max TPS
        let mut window_start = self.window_start.lock().unwrap();
        if window_start.elapsed() >= Duration::from_secs(1) {
            let window_tps = self.window_count.swap(0, Ordering::Relaxed);
            let current_max = self.max_tps_observed.load(Ordering::Relaxed);
            if window_tps > current_max {
                self.max_tps_observed.store(window_tps, Ordering::Relaxed);
            }
            *window_start = Instant::now();
        }
    }

    pub fn record_success(&self, latency_ns: u64) {
        self.total_success.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut hist) = self.latency_hist.lock() {
            // Saturate at histogram max
            let _ = hist.record(latency_ns.min(60_000_000_000));
        }
    }

    pub fn record_error(&self) {
        self.total_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_summary(&self) -> serde_json::Value {
        let total = self.total_success.load(Ordering::Relaxed);
        let errors = self.total_errors.load(Ordering::Relaxed);
        let total_processed = total + errors;
        let error_rate = if total_processed > 0 {
            errors as f64 / total_processed as f64
        } else {
            0.0
        };

        let (p50, p99, p999) = if let Ok(hist) = self.latency_hist.lock() {
            (
                hist.value_at_quantile(0.5),
                hist.value_at_quantile(0.99),
                hist.value_at_quantile(0.999),
            )
        } else {
            (0, 0, 0)
        };

        let elapsed = self.start_time.elapsed().as_secs().max(1);
        let avg_tps = self.total_sent.load(Ordering::Relaxed) / elapsed;
        let max_tps = self.max_tps_observed.load(Ordering::Relaxed).max(avg_tps);

        json!({
            "test_id": self.test_id,
            "total_sent": self.total_sent.load(Ordering::Relaxed),
            "total_success": total,
            "total_errors": errors,
            "error_rate": error_rate,
            "max_tps": max_tps,
            "avg_tps": avg_tps,
            "p50_latency_ns": p50,
            "p99_latency_ns": p99,
            "p999_latency_ns": p999,
            "p99_latency_ms": p99 as f64 / 1_000_000.0,
            "duration_secs": elapsed,
        })
    }

    pub async fn persist_to_db(&self, db_url: &str) -> Result<()> {
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(db_url)
            .await?;

        let summary = self.get_summary();

        let max_tps = summary["max_tps"].as_i64().unwrap_or(0);
        let p50 = summary["p50_latency_ns"].as_i64().unwrap_or(0);
        let p99 = summary["p99_latency_ns"].as_i64().unwrap_or(0);
        let p999 = summary["p999_latency_ns"].as_i64().unwrap_or(0);
        let total_sent = summary["total_sent"].as_i64().unwrap_or(0);
        let total_success = summary["total_success"].as_i64().unwrap_or(0);
        let total_errors = summary["total_errors"].as_i64().unwrap_or(0);
        let error_rate = summary["error_rate"].as_f64().unwrap_or(0.0);

        sqlx::query!(
            r#"
            UPDATE tests SET
                max_tps = $2,
                p50_latency_ns = $3,
                p99_latency_ns = $4,
                p999_latency_ns = $5,
                total_orders = $6,
                successful_orders = $7,
                failed_orders = $8,
                error_rate = $9
            WHERE id = $1
            "#,
            self.test_id,
            max_tps,
            p50,
            p99,
            p999,
            total_sent,
            total_success,
            total_errors,
            error_rate
        )
        .execute(&pool)
        .await?;

        info!("Persisted metrics for test {}: max_tps={}, p99={}ms, error_rate={:.2}%",
            self.test_id, max_tps, p99 / 1_000_000, error_rate * 100.0);

        Ok(())
    }
}