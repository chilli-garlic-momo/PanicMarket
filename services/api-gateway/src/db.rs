use anyhow::Result;
use serde_json::Value;
use sqlx::{PgPool, postgres::PgPoolOptions};
use uuid::Uuid;

#[derive(Clone)]
pub struct Database {
    pool: PgPool,
}

impl Database {
    pub async fn new(url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(20)
            .connect(url)
            .await?;
        Ok(Self { pool })
    }

    pub async fn create_submission(
        &self,
        id: Uuid,
        team_name: &str,
        language: &str,
        minio_key: &str,
    ) -> Result<()> {
        sqlx::query!(
            r#"
            INSERT INTO submissions (id, team_name, language, status, minio_key)
            VALUES ($1, $2, $3, 'queued', $4)
            "#,
            id, team_name, language, minio_key
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_submission(&self, id: Uuid) -> Result<Option<Value>> {
        let row = sqlx::query!(
            r#"
            SELECT 
                id, team_name, language, status,
                minio_key, image_ref, build_log, error_message,
                created_at, updated_at
            FROM submissions
            WHERE id = $1
            "#,
            id
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| {
            serde_json::json!({
                "submission_id": r.id,
                "team_name": r.team_name,
                "language": r.language,
                "status": r.status,
                "minio_key": r.minio_key,
                "image_ref": r.image_ref,
                "build_log": r.build_log,
                "error_message": r.error_message,
                "created_at": r.created_at,
                "updated_at": r.updated_at,
            })
        }))
    }

    pub async fn get_test(&self, id: Uuid) -> Result<Option<Value>> {
        let row = sqlx::query!(
            r#"
            SELECT 
                t.id, t.submission_id, t.status,
                t.max_tps, t.p99_latency_ns, t.p50_latency_ns,
                t.total_orders, t.successful_orders, t.failed_orders,
                t.error_rate, t.throughput_score, t.latency_score,
                t.stability_score, t.final_score,
                t.correctness_passed, t.correctness_score,
                t.engine_endpoint, t.engine_container_id,
                t.timeline, t.failure_reason,
                t.created_at, t.updated_at, t.started_at, t.completed_at
            FROM tests t
            WHERE t.id = $1
            "#,
            id
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| {
            serde_json::json!({
                "test_id": r.id,
                "submission_id": r.submission_id,
                "status": r.status,
                "max_tps": r.max_tps,
                "p99_latency_ns": r.p99_latency_ns,
                "p99_latency_ms": r.p99_latency_ns.map(|ns| ns as f64 / 1_000_000.0),
                "p50_latency_ns": r.p50_latency_ns,
                "total_orders": r.total_orders,
                "successful_orders": r.successful_orders,
                "failed_orders": r.failed_orders,
                "error_rate": r.error_rate,
                "throughput_score": r.throughput_score,
                "latency_score": r.latency_score,
                "stability_score": r.stability_score,
                "final_score": r.final_score,
                "correctness_passed": r.correctness_passed,
                "correctness_score": r.correctness_score,
                "engine_endpoint": r.engine_endpoint,
                "engine_container_id": r.engine_container_id,
                "timeline": r.timeline,
                "failure_reason": r.failure_reason,
                "created_at": r.created_at,
                "updated_at": r.updated_at,
                "started_at": r.started_at,
                "completed_at": r.completed_at,
            })
        }))
    }

    pub async fn get_leaderboard(&self, limit: i64) -> Result<Vec<Value>> {
        let rows = sqlx::query!(
            r#"
            SELECT
                ROW_NUMBER() OVER (ORDER BY t.final_score DESC, t.completed_at ASC) AS rank,
                s.team_name,
                s.id AS submission_id,
                t.id AS test_id,
                t.final_score AS score,
                t.max_tps,
                ROUND((t.p99_latency_ns / 1000000.0)::numeric, 3) AS p99_latency_ms,
                t.error_rate,
                t.correctness_passed,
                t.completed_at AS timestamp
            FROM tests t
            JOIN submissions s ON s.id = t.submission_id
            WHERE t.status = 'completed'
              AND t.final_score IS NOT NULL
            ORDER BY t.final_score DESC, t.completed_at ASC
            LIMIT $1
            "#,
            limit
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|r| {
            serde_json::json!({
                "rank": r.rank,
                "team_name": r.team_name,
                "submission_id": r.submission_id,
                "test_id": r.test_id,
                "score": r.score,
                "max_tps": r.max_tps,
                "p99_latency_ms": r.p99_latency_ms,
                "error_rate": r.error_rate,
                "correctness": if r.correctness_passed.unwrap_or(true) { "pass" } else { "fail" },
                "timestamp": r.timestamp,
            })
        }).collect())
    }
}