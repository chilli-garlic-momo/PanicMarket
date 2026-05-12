use anyhow::Result;
use sqlx::{PgPool, postgres::PgPoolOptions};
use uuid::Uuid;

#[derive(Clone)]
pub struct Database {
    pool: PgPool,
}

impl Database {
    pub async fn new(url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(url)
            .await?;
        Ok(Self { pool })
    }

    pub async fn update_submission_status(
        &self,
        id: Uuid,
        status: &str,
        image_ref: Option<&str>,
        error_message: Option<&str>,
    ) -> Result<()> {
        sqlx::query!(
            r#"
            UPDATE submissions
            SET status = $2, image_ref = COALESCE($3, image_ref),
                error_message = COALESCE($4, error_message)
            WHERE id = $1
            "#,
            id, status, image_ref, error_message
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn append_build_log(&self, id: Uuid, log: &str) -> Result<()> {
        sqlx::query!(
            r#"
            UPDATE submissions
            SET build_log = COALESCE(build_log, '') || $2
            WHERE id = $1
            "#,
            id, log
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}