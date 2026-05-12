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
            .max_connections(10)
            .connect(url)
            .await?;
        Ok(Self { pool })
    }

    pub async fn create_test(&self, submission_id: Uuid) -> Result<Uuid> {
        let row = sqlx::query!(
            r#"
            INSERT INTO tests (submission_id, status)
            VALUES ($1, 'pending')
            RETURNING id
            "#,
            submission_id
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(row.id)
    }

    pub async fn update_submission_status(&self, id: Uuid, status: &str) -> Result<()> {
        sqlx::query!("UPDATE submissions SET status = $2 WHERE id = $1", id, status)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn update_test_engine(
        &self,
        test_id: Uuid,
        endpoint: &str,
        container_id: &str,
    ) -> Result<()> {
        sqlx::query!(
            r#"
            UPDATE tests
            SET engine_endpoint = $2, engine_container_id = $3
            WHERE id = $1
            "#,
            test_id, endpoint, container_id
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn start_test(&self, test_id: Uuid) -> Result<()> {
        sqlx::query!(
            r#"UPDATE tests SET status = 'running', started_at = NOW() WHERE id = $1"#,
            test_id
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn fail_test(&self, test_id: Uuid, reason: &str, message: &str) -> Result<()> {
        sqlx::query!(
            r#"
            UPDATE tests
            SET status = 'failed', failure_reason = $2, final_score = 0,
                completed_at = NOW()
            WHERE id = $1
            "#,
            test_id,
            format!("{}: {}", reason, message)
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn complete_test(&self, test_id: Uuid) -> Result<()> {
        sqlx::query!(
            r#"
            UPDATE tests
            SET status = 'completed', completed_at = NOW()
            WHERE id = $1 AND status != 'failed'
            "#,
            test_id
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn append_timeline(&self, test_id: Uuid, event: Value) -> Result<()> {
        sqlx::query!(
            r#"
            UPDATE tests
            SET timeline = timeline || $2::jsonb
            WHERE id = $1
            "#,
            test_id,
            serde_json::json!([event])
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_test_metrics(&self, test_id: Uuid) -> Result<Value> {
        let row = sqlx::query!(
            r#"
            SELECT max_tps, p99_latency_ns, total_orders, error_rate
            FROM tests WHERE id = $1
            "#,
            test_id
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| serde_json::json!({
            "max_tps": r.max_tps,
            "p99_latency_ns": r.p99_latency_ns,
            "total_orders": r.total_orders,
            "error_rate": r.error_rate,
        })).unwrap_or_default())
    }

    pub async fn find_stale_built_submissions(&self) -> Result<Vec<(Uuid, String)>> {
        let rows = sqlx::query!(
            r#"
            SELECT s.id, s.image_ref
            FROM submissions s
            WHERE s.status = 'built'
              AND s.image_ref IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM tests t
                WHERE t.submission_id = s.id
                  AND t.status NOT IN ('failed')
              )
              AND s.updated_at < NOW() - INTERVAL '2 minutes'
            LIMIT 5
            "#
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter()
            .filter_map(|r| r.image_ref.map(|img| (r.id, img)))
            .collect())
    }
}