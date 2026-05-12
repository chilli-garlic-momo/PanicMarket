//! Orchestrator: Manages the full benchmark lifecycle
//! 
//! For MVP: polls for 'built' submissions and runs the full workflow
//! Phase 2: Replace with Temporal workflow engine

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tracing::{error, info, warn};
use uuid::Uuid;

mod db;
mod deployer;
mod workflow;

use db::Database;
use deployer::Deployer;
use workflow::BenchmarkWorkflow;

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
    let bot_worker_url = std::env::var("BOT_WORKER_URL")
        .unwrap_or_else(|_| "http://localhost:9090".to_string());
    let scoring_engine_url = std::env::var("SCORING_ENGINE_URL")
        .unwrap_or_else(|_| "http://localhost:9091".to_string());
    let deployment_mode = std::env::var("DEPLOYMENT_MODE")
        .unwrap_or_else(|_| "docker".to_string());

    info!("Orchestrator starting (mode: {})...", deployment_mode);

    let db = Arc::new(Database::new(&database_url).await?);
    let deployer = Arc::new(Deployer::new(&deployment_mode).await?);

    // Max 3 concurrent tests for MVP
    let semaphore = Arc::new(Semaphore::new(3));

    let redis_client = redis::Client::open(redis_url.as_str())?;
    let mut redis_conn = redis_client.get_multiplexed_async_connection().await?;

    info!("Orchestrator ready. Watching for built submissions...");

    loop {
        // Listen for orchestration signals from build worker
        let result: Option<(String, String)> = redis::cmd("BRPOP")
            .arg("orchestrate:queue")
            .arg(5)
            .query_async(&mut redis_conn)
            .await
            .unwrap_or(None);

        if let Some((_key, payload)) = result {
            #[derive(Deserialize)]
            struct OrchestrateJob {
                submission_id: Uuid,
                image_ref: String,
            }

            match serde_json::from_str::<OrchestrateJob>(&payload) {
                Ok(job) => {
                    info!("Orchestrating benchmark for submission {}", job.submission_id);

                    let db_clone = db.clone();
                    let deployer_clone = deployer.clone();
                    let bot_worker_url = bot_worker_url.clone();
                    let scoring_engine_url = scoring_engine_url.clone();
                    let sem = semaphore.clone();

                    tokio::spawn(async move {
                        let _permit = sem.acquire().await.expect("Semaphore closed");
                        let workflow = BenchmarkWorkflow::new(
                            db_clone,
                            deployer_clone,
                            bot_worker_url,
                            scoring_engine_url,
                        );

                        if let Err(e) = workflow.run(job.submission_id, job.image_ref).await {
                            error!("Benchmark workflow failed for {}: {}", job.submission_id, e);
                        }
                    });
                }
                Err(e) => {
                    error!("Failed to parse orchestrate job: {} - {}", e, payload);
                }
            }
        }

        // Also poll DB for any 'built' submissions not picked up via Redis
        // (recovery path in case of restart)
        if let Ok(stale) = db.find_stale_built_submissions().await {
            for (submission_id, image_ref) in stale {
                warn!("Found stale built submission {} - re-queuing", submission_id);
                let job = serde_json::json!({
                    "submission_id": submission_id,
                    "image_ref": image_ref,
                });
                redis::cmd("LPUSH")
                    .arg("orchestrate:queue")
                    .arg(job.to_string())
                    .query_async::<()>(&mut redis_conn)
                    .await
                    .unwrap_or(());
            }
        }
    }
}