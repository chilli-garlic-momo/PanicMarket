use anyhow::Result;
use axum::{
    extract::State,
    response::Json,
    routing::post,
    Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::PgPool;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{info, warn};
use uuid::Uuid;

mod scorer;
use scorer::compute_score;

#[derive(Clone)]
struct AppState {
    pool: PgPool,
    redis_url: String,
    leaderboard_api_url: String,
}

#[derive(Deserialize)]
struct ScoreRequest {
    test_id: Uuid,
    metrics: Value,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://benchmark:benchmark_secret@localhost:5432/benchmark".to_string());
    let redis_url = std::env::var("REDIS_URL")
        .unwrap_or_else(|_| "redis://localhost:6379".to_string());
    let leaderboard_api_url = std::env::var("LEADERBOARD_API_URL")
        .unwrap_or_else(|_| "http://localhost:9092".to_string());

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await?;

    let state = AppState { pool, redis_url, leaderboard_api_url };

    let app = Router::new()
        .route("/score", post(score_test))
        .route("/health", axum::routing::get(|| async { Json(json!({"status": "ok"})) }))
        .with_state(Arc::new(state));

    let addr = "0.0.0.0:9091";
    info!("Scoring Engine listening on {}", addr);
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn score_test(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ScoreRequest>,
) -> Json<Value> {
    let test_id = req.test_id;
    info!("Computing score for test {}", test_id);

    // Load latest metrics from DB (bot worker may have persisted them)
    let db_metrics = load_test_metrics(&state.pool, test_id).await;
    
    // Merge: prefer DB metrics if available, fall back to request metrics
    let max_tps = db_metrics.get("max_tps")
        .and_then(|v| v.as_i64())
        .or_else(|| req.metrics.get("max_tps").and_then(|v| v.as_i64()))
        .unwrap_or(0);
    
    let p99_latency_ns = db_metrics.get("p99_latency_ns")
        .and_then(|v| v.as_i64())
        .or_else(|| req.metrics.get("p99_latency_ns").and_then(|v| v.as_i64()))
        .unwrap_or(0);
    
    let error_rate = db_metrics.get("error_rate")
        .and_then(|v| v.as_f64())
        .or_else(|| req.metrics.get("error_rate").and_then(|v| v.as_f64()))
        .unwrap_or(0.0);

    let correctness_passed = db_metrics.get("correctness_passed")
        .and_then(|v| v.as_bool())
        .unwrap_or(true); // MVP: assume pass

    // Compute scores
    let (throughput_score, latency_score, stability_score, final_score) =
        compute_score(max_tps, p99_latency_ns, error_rate, correctness_passed);

    // Persist scores
    if let Err(e) = save_scores(
        &state.pool,
        test_id,
        throughput_score,
        latency_score,
        stability_score,
        final_score,
    ).await {
        warn!("Failed to save scores for test {}: {}", test_id, e);
    }

    // Update leaderboard cache in Redis
    update_leaderboard_cache(&state.redis_url, test_id, final_score).await;

    // Notify leaderboard API
    notify_leaderboard(&state.leaderboard_api_url, test_id, final_score, max_tps, p99_latency_ns, error_rate).await;

    info!(
        "Test {} scored: throughput={:.1} latency={:.1} stability={:.1} final={:.1}",
        test_id, throughput_score, latency_score, stability_score, final_score
    );

    Json(json!({
        "test_id": test_id,
        "throughput_score": throughput_score,
        "latency_score": latency_score,
        "stability_score": stability_score,
        "final_score": final_score,
        "correctness_passed": correctness_passed,
    }))
}

async fn load_test_metrics(pool: &PgPool, test_id: Uuid) -> Value {
    sqlx::query!(
        r#"
        SELECT max_tps, p99_latency_ns, error_rate, correctness_passed
        FROM tests WHERE id = $1
        "#,
        test_id
    )
    .fetch_optional(pool)
    .await
    .ok()
    .flatten()
    .map(|r| json!({
        "max_tps": r.max_tps,
        "p99_latency_ns": r.p99_latency_ns,
        "error_rate": r.error_rate,
        "correctness_passed": r.correctness_passed,
    }))
    .unwrap_or_default()
}

async fn save_scores(
    pool: &PgPool,
    test_id: Uuid,
    throughput: f64,
    latency: f64,
    stability: f64,
    final_score: f64,
) -> Result<()> {
    sqlx::query!(
        r#"
        UPDATE tests SET
            throughput_score = $2,
            latency_score = $3,
            stability_score = $4,
            final_score = $5
        WHERE id = $1
        "#,
        test_id, throughput, latency, stability, final_score
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn update_leaderboard_cache(redis_url: &str, test_id: Uuid, score: f64) {
    if let Ok(client) = redis::Client::open(redis_url) {
        if let Ok(mut conn) = client.get_multiplexed_async_connection().await {
            let _: Result<(), _> = redis::cmd("ZADD")
                .arg("leaderboard:scores")
                .arg(score)
                .arg(test_id.to_string())
                .query_async(&mut conn)
                .await;
        }
    }
}

async fn notify_leaderboard(
    leaderboard_url: &str,
    test_id: Uuid,
    score: f64,
    max_tps: i64,
    p99_ns: i64,
    error_rate: f64,
) {
    let client = reqwest::Client::new();
    let _ = client.post(&format!("{}/notify", leaderboard_url))
        .json(&json!({
            "test_id": test_id,
            "score": score,
            "max_tps": max_tps,
            "p99_latency_ms": p99_ns as f64 / 1_000_000.0,
            "error_rate": error_rate,
        }))
        .send()
        .await;
}