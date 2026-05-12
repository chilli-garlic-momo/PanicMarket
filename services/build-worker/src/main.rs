use anyhow::Result;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::path::PathBuf;
use tracing::{error, info, warn};
use uuid::Uuid;

mod db;
mod docker_builder;
mod minio_client;

use db::Database;
use docker_builder::DockerBuilder;
use minio_client::MinioClient;

#[derive(Debug, Deserialize)]
struct BuildJob {
    submission_id: Uuid,
    minio_key: String,
    language: String,
    team_name: String,
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
    let minio_endpoint = std::env::var("MINIO_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:9000".to_string());
    let minio_access_key = std::env::var("MINIO_ACCESS_KEY")
        .unwrap_or_else(|_| "minioadmin".to_string());
    let minio_secret_key = std::env::var("MINIO_SECRET_KEY")
        .unwrap_or_else(|_| "minioadmin123".to_string());
    let minio_bucket = std::env::var("MINIO_BUCKET")
        .unwrap_or_else(|_| "submissions".to_string());
    let registry_url = std::env::var("REGISTRY_URL")
        .unwrap_or_else(|_| "localhost:5001".to_string());

    info!("Build Worker starting...");

    let db = Database::new(&database_url).await?;
    let minio = MinioClient::new(&minio_endpoint, &minio_access_key, &minio_secret_key, &minio_bucket).await?;
    let builder = DockerBuilder::new(&registry_url).await?;

    let redis_client = redis::Client::open(redis_url.as_str())?;
    let mut redis_conn = redis_client.get_multiplexed_async_connection().await?;

    info!("Build Worker ready. Polling build:queue...");

    loop {
        // BRPOP with 5s timeout (blocking right-pop)
        let result: Option<(String, String)> = redis::cmd("BRPOP")
            .arg("build:queue")
            .arg(5)
            .query_async(&mut redis_conn)
            .await
            .unwrap_or(None);

        if let Some((_key, payload)) = result {
            match serde_json::from_str::<BuildJob>(&payload) {
                Ok(job) => {
                    info!("Processing build job for submission {}", job.submission_id);
                    if let Err(e) = process_build_job(&db, &minio, &builder, job).await {
                        error!("Build job failed: {}", e);
                    }
                }
                Err(e) => {
                    error!("Failed to deserialize build job: {} - payload: {}", e, payload);
                }
            }
        }
    }
}

async fn process_build_job(
    db: &Database,
    minio: &MinioClient,
    builder: &DockerBuilder,
    job: BuildJob,
) -> Result<()> {
    let submission_id = job.submission_id;

    // Update status to building
    db.update_submission_status(submission_id, "building", None, None).await?;
    db.append_build_log(submission_id, "=== Build started ===\n").await?;

    // Download from MinIO
    info!("Downloading submission {} from MinIO key {}", submission_id, job.minio_key);
    db.append_build_log(submission_id, "Downloading source code...\n").await?;

    let archive_data = minio.download(&job.minio_key).await
        .map_err(|e| {
            anyhow::anyhow!("Failed to download from MinIO: {}", e)
        })?;

    // Extract to temp dir
    let build_dir = extract_archive(&archive_data, submission_id).await?;
    db.append_build_log(submission_id, "Source code extracted successfully.\n").await?;

    // Validate Dockerfile exists
    let dockerfile_path = build_dir.join("Dockerfile");
    if !dockerfile_path.exists() {
        let msg = "ERROR: Dockerfile not found in submission root\n";
        db.append_build_log(submission_id, msg).await?;
        db.update_submission_status(submission_id, "failed", None, Some("Missing Dockerfile")).await?;
        return Ok(());
    }

    // Build Docker image
    let image_tag = format!("localhost:5001/submission-{}", submission_id);
    db.append_build_log(submission_id, &format!("Building Docker image: {}...\n", image_tag)).await?;

    match builder.build_image(&build_dir, &image_tag, submission_id).await {
        Ok(build_log) => {
            db.append_build_log(submission_id, &build_log).await?;
            db.append_build_log(submission_id, "=== Build succeeded ===\n").await?;

            // Update to built status with image ref
            db.update_submission_status(submission_id, "built", Some(&image_tag), None).await?;

            // Queue for orchestration (Temporal will pick this up)
            info!("Submission {} built successfully as {}", submission_id, image_tag);

            // Signal orchestrator via Redis
            let orchestrate_job = serde_json::json!({
                "submission_id": submission_id,
                "image_ref": image_tag,
            });
            // Note: orchestrator polls submissions table, but also accepts Redis signal
            let redis_client = redis::Client::open(
                std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string())
            )?;
            let mut conn = redis_client.get_multiplexed_async_connection().await?;
            redis::cmd("LPUSH")
                .arg("orchestrate:queue")
                .arg(orchestrate_job.to_string())
                .query_async::<()>(&mut conn)
                .await?;

            Ok(())
        }
        Err(e) => {
            let error_msg = format!("Build failed: {}\n", e);
            db.append_build_log(submission_id, &error_msg).await?;
            db.update_submission_status(submission_id, "failed", None, Some(&e.to_string())).await?;
            Err(e)
        }
    }
}

async fn extract_archive(data: &[u8], submission_id: Uuid) -> Result<PathBuf> {
    let build_dir = PathBuf::from(format!("/tmp/builds/{}", submission_id));
    tokio::fs::create_dir_all(&build_dir).await?;

    let cursor = std::io::Cursor::new(data);
    let gz = flate2::read::GzDecoder::new(cursor);
    let mut archive = tar::Archive::new(gz);
    archive.unpack(&build_dir)?;

    Ok(build_dir)
}