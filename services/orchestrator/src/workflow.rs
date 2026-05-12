use anyhow::Result;
use chrono::Utc;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::db::Database;
use crate::deployer::Deployer;

pub struct BenchmarkWorkflow {
    db: Arc<Database>,
    deployer: Arc<Deployer>,
    bot_worker_url: String,
    scoring_engine_url: String,
}

impl BenchmarkWorkflow {
    pub fn new(
        db: Arc<Database>,
        deployer: Arc<Deployer>,
        bot_worker_url: String,
        scoring_engine_url: String,
    ) -> Self {
        Self { db, deployer, bot_worker_url, scoring_engine_url }
    }

    pub async fn run(&self, submission_id: Uuid, image_ref: String) -> Result<()> {
        info!("Starting benchmark workflow for submission {}", submission_id);

        // Create test record
        let test_id = self.db.create_test(submission_id).await?;
        self.add_timeline_event(test_id, "test_created", json!({})).await;

        // Update submission status
        self.db.update_submission_status(submission_id, "deploying").await?;

        // Step 1: Deploy engine
        info!("[{}] Deploying engine from image {}", test_id, image_ref);
        let (endpoint, container_id) = match self.deployer.deploy(&image_ref, test_id).await {
            Ok(result) => result,
            Err(e) => {
                error!("[{}] Deploy failed: {}", test_id, e);
                self.add_timeline_event(test_id, "deploy_failed", json!({"error": e.to_string()})).await;
                self.db.fail_test(test_id, "deploy_failed", &e.to_string()).await?;
                self.db.update_submission_status(submission_id, "failed").await?;
                return Err(e);
            }
        };

        self.db.update_test_engine(test_id, &endpoint, &container_id).await?;
        self.add_timeline_event(test_id, "engine_deployed", json!({"endpoint": endpoint})).await;

        // Step 2: Health check
        info!("[{}] Running health check on {}", test_id, endpoint);
        if let Err(e) = self.health_check(&endpoint).await {
            error!("[{}] Health check failed: {}", test_id, e);
            self.add_timeline_event(test_id, "health_check_failed", json!({"error": e.to_string()})).await;
            self.db.fail_test(test_id, "health_check_failed", &e.to_string()).await?;
            self.db.update_submission_status(submission_id, "failed").await?;
            self.deployer.cleanup(&container_id).await.unwrap_or(());
            return Err(e);
        }

        self.add_timeline_event(test_id, "health_check_passed", json!({})).await;
        self.db.update_submission_status(submission_id, "testing").await?;
        self.db.start_test(test_id).await?;

        // Step 3: Start bot test
        info!("[{}] Starting bot test against {}", test_id, endpoint);
        let http = reqwest::Client::new();

        let start_resp = http.post(&format!("{}/start", self.bot_worker_url))
            .json(&json!({
                "test_id": test_id,
                "engine_endpoint": endpoint,
                "duration_secs": 80,
                "bot_count": 100,
                "target_tps": 1000,
            }))
            .send()
            .await;

        match start_resp {
            Ok(resp) if resp.status().is_success() => {
                self.add_timeline_event(test_id, "test_started", json!({})).await;
                info!("[{}] Bot test started", test_id);
            }
            Ok(resp) => {
                let body = resp.text().await.unwrap_or_default();
                return Err(anyhow::anyhow!("Bot worker rejected test start: {}", body));
            }
            Err(e) => {
                return Err(anyhow::anyhow!("Failed to contact bot worker: {}", e));
            }
        }

        // Step 4: Wait for test to complete (80s + buffer)
        info!("[{}] Waiting for test to complete...", test_id);
        sleep(Duration::from_secs(85)).await;

        // Step 5: Stop test and collect results
        info!("[{}] Collecting test results", test_id);
        let results = http.post(&format!("{}/stop", self.bot_worker_url))
            .json(&json!({"test_id": test_id}))
            .send()
            .await;

        let metrics = match results {
            Ok(resp) if resp.status().is_success() => {
                resp.json::<serde_json::Value>().await.unwrap_or_default()
            }
            _ => {
                // Try to get partial results from DB
                self.db.get_test_metrics(test_id).await.unwrap_or_default()
            }
        };

        self.add_timeline_event(test_id, "test_completed", json!({})).await;

        // Step 6: Score
        info!("[{}] Computing score", test_id);
        let score_resp = http.post(&format!("{}/score", self.scoring_engine_url))
            .json(&json!({
                "test_id": test_id,
                "metrics": metrics,
            }))
            .send()
            .await;

        match score_resp {
            Ok(resp) if resp.status().is_success() => {
                self.add_timeline_event(test_id, "score_computed", json!({})).await;
                self.db.complete_test(test_id).await?;
                self.db.update_submission_status(submission_id, "completed").await?;
                info!("[{}] Benchmark complete", test_id);
            }
            Ok(resp) => {
                let body = resp.text().await.unwrap_or_default();
                warn!("[{}] Scoring issue: {}", test_id, body);
                // Still mark complete
                self.db.complete_test(test_id).await?;
                self.db.update_submission_status(submission_id, "completed").await?;
            }
            Err(e) => {
                error!("[{}] Failed to contact scoring engine: {}", test_id, e);
            }
        }

        // Step 7: Cleanup
        info!("[{}] Cleaning up engine container {}", test_id, container_id);
        if let Err(e) = self.deployer.cleanup(&container_id).await {
            warn!("[{}] Cleanup failed (non-fatal): {}", test_id, e);
        }

        Ok(())
    }

    async fn health_check(&self, endpoint: &str) -> Result<()> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()?;

        let health_url = format!("{}/health", endpoint);
        let mut attempts = 0;
        let max_attempts = 10;

        loop {
            attempts += 1;
            match http.get(&health_url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    info!("Health check passed on attempt {}", attempts);
                    return Ok(());
                }
                Ok(resp) => {
                    warn!("Health check attempt {}/{}: status {}", attempts, max_attempts, resp.status());
                }
                Err(e) => {
                    warn!("Health check attempt {}/{}: {}", attempts, max_attempts, e);
                }
            }

            if attempts >= max_attempts {
                return Err(anyhow::anyhow!("Health check failed after {} attempts", max_attempts));
            }

            // Exponential backoff: 3s, 3s, 6s, 6s, 12s...
            let delay = if attempts <= 2 { 3 } else if attempts <= 4 { 6 } else { 12 };
            sleep(Duration::from_secs(delay)).await;
        }
    }

    async fn add_timeline_event(&self, test_id: Uuid, event: &str, data: serde_json::Value) {
        let entry = json!({
            "time": Utc::now().to_rfc3339(),
            "event": event,
            "data": data,
        });

        if let Err(e) = self.db.append_timeline(test_id, entry).await {
            warn!("Failed to append timeline event {}: {}", event, e);
        }
    }
}