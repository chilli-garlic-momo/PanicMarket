//! Bot Worker: Generates load against trading engines
//! 
//! HTTP API for control + internal bot runner
//! MVP: 100 bots, 1000 TPS target

use anyhow::Result;
use axum::{
    extract::State,
    response::Json,
    routing::post,
    Router,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, RwLock};
use tracing::{error, info};
use uuid::Uuid;
use std::collections::HashMap;

mod bot;
mod metrics;
mod order_gen;

use bot::BotFleet;
use metrics::TestMetrics;

#[derive(Clone)]
struct AppState {
    active_tests: Arc<RwLock<HashMap<Uuid, Arc<TestRun>>>>,
    db_url: String,
}

struct TestRun {
    test_id: Uuid,
    fleet: Arc<BotFleet>,
    metrics: Arc<TestMetrics>,
}

#[derive(Deserialize)]
struct StartTestRequest {
    test_id: Uuid,
    engine_endpoint: String,
    duration_secs: u64,
    bot_count: usize,
    target_tps: u64,
}

#[derive(Deserialize)]
struct StopTestRequest {
    test_id: Uuid,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://benchmark:benchmark_secret@localhost:5432/benchmark".to_string());

    let state = AppState {
        active_tests: Arc::new(RwLock::new(HashMap::new())),
        db_url: database_url,
    };

    let app = Router::new()
        .route("/start", post(start_test))
        .route("/stop", post(stop_test))
        .route("/health", axum::routing::get(|| async { Json(json!({"status": "ok"})) }))
        .with_state(Arc::new(state));

    let addr = "0.0.0.0:9090";
    info!("Bot Worker listening on {}", addr);
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn start_test(
    State(state): State<Arc<AppState>>,
    Json(req): Json<StartTestRequest>,
) -> Json<serde_json::Value> {
    let test_id = req.test_id;
    info!("Starting test {} against {} with {} bots", test_id, req.engine_endpoint, req.bot_count);

    let metrics = Arc::new(TestMetrics::new(test_id));
    let fleet = Arc::new(BotFleet::new(
        req.bot_count,
        req.target_tps,
        req.engine_endpoint.clone(),
        metrics.clone(),
    ));

    let run = Arc::new(TestRun {
        test_id,
        fleet: fleet.clone(),
        metrics: metrics.clone(),
    });

    state.active_tests.write().await.insert(test_id, run);

    let fleet_clone = fleet.clone();
    let duration = req.duration_secs;
    let db_url = state.db_url.clone();
    let metrics_clone = metrics.clone();
    let active_tests = state.active_tests.clone();

    tokio::spawn(async move {
        // Run the fleet for duration
        fleet_clone.run(duration).await;

        // Persist final metrics to DB
        if let Err(e) = metrics_clone.persist_to_db(&db_url).await {
            error!("Failed to persist metrics for test {}: {}", test_id, e);
        }

        // Remove from active tests
        active_tests.write().await.remove(&test_id);
        info!("Test {} completed", test_id);
    });

    Json(json!({
        "status": "started",
        "test_id": test_id,
    }))
}

async fn stop_test(
    State(state): State<Arc<AppState>>,
    Json(req): Json<StopTestRequest>,
) -> Json<serde_json::Value> {
    let test_id = req.test_id;

    let run = state.active_tests.read().await.get(&test_id).cloned();

    if let Some(run) = run {
        run.fleet.stop().await;
        let summary = run.metrics.get_summary();
        info!("Stopped test {}: {:?}", test_id, summary);
        Json(json!({
            "status": "stopped",
            "test_id": test_id,
            "metrics": summary,
        }))
    } else {
        // Test might have already finished - return from DB
        Json(json!({
            "status": "already_completed",
            "test_id": test_id,
        }))
    }
}