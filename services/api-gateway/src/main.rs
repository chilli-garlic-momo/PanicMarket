use anyhow::Result;
use axum::{
    extract::{Multipart, Path, Query, State},
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Json, Response},
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::PgPool;
use std::sync::Arc;
use tokio::net::TcpListener;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;
use tracing::{error, info, warn};
use uuid::Uuid;

mod db;
mod error;
mod minio_client;
mod redis_client;

use db::Database;
use error::AppError;
use minio_client::MinioClient;
use redis_client::RedisClient;

#[derive(Clone)]
pub struct AppState {
    pub db: Database,
    pub redis: RedisClient,
    pub minio: MinioClient,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("api_gateway=debug".parse()?)
                .add_directive("tower_http=debug".parse()?),
        )
        .init();

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgresql://benchmark:benchmark_secret@localhost:5432/benchmark".to_string());
    let redis_url = std::env::var("REDIS_URL")
        .unwrap_or_else(|_| "redis://localhost:6379".to_string());
    let minio_endpoint = std::env::var("MINIO_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:9000".to_string());
    let minio_access_key = std::env::var("MINIO_ACCESS_KEY")
        .unwrap_or_else(|_| "minioadmin".to_string());
    let minio_secret_key = std::env::var("MINIO_SECRET_KEY")
        .unwrap_or_else(|_| "minioadmin123".to_string());
    let minio_bucket = std::env::var("MINIO_BUCKET")
        .unwrap_or_else(|_| "submissions".to_string());

    info!("Connecting to PostgreSQL...");
    let db = Database::new(&database_url).await?;

    info!("Connecting to Redis...");
    let redis = RedisClient::new(&redis_url).await?;

    info!("Connecting to MinIO...");
    let minio = MinioClient::new(
        &minio_endpoint,
        &minio_access_key,
        &minio_secret_key,
        &minio_bucket,
    ).await?;

    let state = AppState { db, redis, minio };

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers(Any);

    let app = Router::new()
        // Submissions
        .route("/api/v1/submissions", post(create_submission))
        .route("/api/v1/submissions/:id", get(get_submission))
        .route("/api/v1/submissions/:id/build-logs", get(get_build_logs))
        // Tests
        .route("/api/v1/tests/:id", get(get_test))
        .route("/api/v1/tests/:id/logs", get(get_test_logs))
        .route("/api/v1/tests/:id/debug", get(get_test_debug))
        // Leaderboard
        .route("/api/v1/leaderboard", get(get_leaderboard))
        // Health
        .route("/health", get(health_check))
        .with_state(Arc::new(state))
        .layer(TraceLayer::new_for_http())
        .layer(cors);

    let addr = "0.0.0.0:8080";
    info!("API Gateway listening on {}", addr);
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn health_check() -> impl IntoResponse {
    Json(json!({"status": "ok", "service": "api-gateway"}))
}

// POST /api/v1/submissions
async fn create_submission(
    State(state): State<Arc<AppState>>,
    mut multipart: Multipart,
) -> Result<impl IntoResponse, AppError> {
    let mut code_bytes: Option<bytes::Bytes> = None;
    let mut team_name = String::from("unknown");
    let mut language = String::from("rust");

    while let Some(field) = multipart.next_field().await? {
        let name = field.name().unwrap_or("").to_string();
        match name.as_str() {
            "code" => {
                let data = field.bytes().await?;
                if data.len() > 100 * 1024 * 1024 {
                    return Err(AppError::BadRequest("File exceeds 100MB limit".to_string()));
                }
                code_bytes = Some(data);
            }
            "team_name" => {
                team_name = field.text().await?;
                if team_name.is_empty() || team_name.len() > 255 {
                    return Err(AppError::BadRequest("team_name must be 1-255 chars".to_string()));
                }
            }
            "language" => {
                language = field.text().await?;
                if !["rust", "cpp", "go", "python"].contains(&language.as_str()) {
                    return Err(AppError::BadRequest(format!("Unsupported language: {}", language)));
                }
            }
            _ => {
                // Ignore unknown fields
                let _ = field.bytes().await;
            }
        }
    }

    let code_bytes = code_bytes
        .ok_or_else(|| AppError::BadRequest("Missing 'code' field (tar.gz file)".to_string()))?;

    // Validate it looks like gzip
    if code_bytes.len() < 2 || code_bytes[0] != 0x1f || code_bytes[1] != 0x8b {
        return Err(AppError::BadRequest("File must be a valid .tar.gz archive".to_string()));
    }

    let submission_id = Uuid::new_v4();
    let minio_key = format!("submissions/{}/{}.tar.gz", submission_id, submission_id);

    // Upload to MinIO
    state.minio.upload(&minio_key, code_bytes.to_vec()).await
        .map_err(|e| {
            error!("MinIO upload failed: {}", e);
            AppError::Internal("Storage upload failed".to_string())
        })?;

    // Store in database
    state.db.create_submission(submission_id, &team_name, &language, &minio_key).await
        .map_err(|e| {
            error!("DB insert failed: {}", e);
            AppError::Internal("Database error".to_string())
        })?;

    // Queue build job
    let build_job = serde_json::json!({
        "submission_id": submission_id,
        "minio_key": minio_key,
        "language": language,
        "team_name": team_name,
    });
    state.redis.lpush("build:queue", build_job.to_string()).await
        .map_err(|e| {
            error!("Redis enqueue failed: {}", e);
            AppError::Internal("Queue error".to_string())
        })?;

    info!("Submission {} queued for building (team: {})", submission_id, team_name);

    Ok((
        StatusCode::CREATED,
        Json(json!({
            "submission_id": submission_id,
            "status": "queued",
            "message": "Submission accepted. Build will start shortly.",
            "links": {
                "status": format!("/api/v1/submissions/{}", submission_id),
                "build_logs": format!("/api/v1/submissions/{}/build-logs", submission_id),
            }
        })),
    ))
}

// GET /api/v1/submissions/:id
async fn get_submission(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, AppError> {
    let submission = state.db.get_submission(id).await?
        .ok_or(AppError::NotFound("Submission not found".to_string()))?;

    Ok(Json(submission))
}

// GET /api/v1/submissions/:id/build-logs
async fn get_build_logs(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, AppError> {
    let submission = state.db.get_submission(id).await?
        .ok_or(AppError::NotFound("Submission not found".to_string()))?;

    let logs = submission.get("build_log")
        .and_then(|v| v.as_str())
        .unwrap_or("No build logs available yet.")
        .to_string();

    Ok((
        [(axum::http::header::CONTENT_TYPE, "text/plain")],
        logs,
    ))
}

// GET /api/v1/tests/:id
async fn get_test(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, AppError> {
    let test = state.db.get_test(id).await?
        .ok_or(AppError::NotFound("Test not found".to_string()))?;

    Ok(Json(test))
}

// GET /api/v1/tests/:id/logs
async fn get_test_logs(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> Result<Response, AppError> {
    let test = state.db.get_test(id).await?
        .ok_or(AppError::NotFound("Test not found".to_string()))?;

    // In MVP: return placeholder; Phase 2 will stream from container logs
    let container_id = test.get("engine_container_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let logs = format!(
        "Test ID: {}\nContainer: {}\nStatus: {}\n\nNote: Full log streaming available in Phase 2.",
        id,
        container_id,
        test.get("status").and_then(|v| v.as_str()).unwrap_or("unknown")
    );

    Ok((
        [(axum::http::header::CONTENT_TYPE, "text/plain")],
        logs,
    ).into_response())
}

// GET /api/v1/tests/:id/debug
async fn get_test_debug(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, AppError> {
    let test = state.db.get_test(id).await?
        .ok_or(AppError::NotFound("Test not found".to_string()))?;

    let failure_reason = test.get("failure_reason")
        .and_then(|v| v.as_str())
        .unwrap_or("none");

    let timeline = test.get("timeline").cloned().unwrap_or(json!([]));

    let hints = generate_hints(&test);

    Ok(Json(json!({
        "test_id": id,
        "status": test.get("status"),
        "failure_reason": failure_reason,
        "timeline": timeline,
        "max_tps_achieved": test.get("max_tps"),
        "p99_latency_ms": test.get("p99_latency_ns")
            .and_then(|v| v.as_i64())
            .map(|ns| ns as f64 / 1_000_000.0),
        "hints": hints,
    })))
}

fn generate_hints(test: &Value) -> Vec<String> {
    let mut hints = vec![];

    if let Some(error_rate) = test.get("error_rate").and_then(|v| v.as_f64()) {
        if error_rate > 0.05 {
            hints.push(format!("High error rate ({:.1}%). Check engine response handling.", error_rate * 100.0));
        }
    }

    if let Some(p99_ns) = test.get("p99_latency_ns").and_then(|v| v.as_i64()) {
        let p99_ms = p99_ns as f64 / 1_000_000.0;
        if p99_ms > 100.0 {
            hints.push(format!("High p99 latency ({:.1}ms). Consider reducing lock contention.", p99_ms));
        }
    }

    if hints.is_empty() {
        hints.push("No specific issues detected.".to_string());
    }

    hints
}

#[derive(Debug, Deserialize)]
struct LeaderboardQuery {
    limit: Option<i64>,
}

// GET /api/v1/leaderboard
async fn get_leaderboard(
    State(state): State<Arc<AppState>>,
    Query(params): Query<LeaderboardQuery>,
) -> Result<impl IntoResponse, AppError> {
    let limit = params.limit.unwrap_or(100).min(1000);
    let entries = state.db.get_leaderboard(limit).await?;

    Ok(Json(json!({
        "entries": entries,
        "total": entries.len(),
    })))
}