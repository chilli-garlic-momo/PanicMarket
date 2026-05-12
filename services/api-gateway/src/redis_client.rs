use anyhow::Result;
use redis::{AsyncCommands, Client, aio::ConnectionManager};

#[derive(Clone)]
pub struct RedisClient {
    manager: ConnectionManager,
}

impl RedisClient {
    pub async fn new(url: &str) -> Result<Self> {
        let client = Client::open(url)?;
        let manager = ConnectionManager::new(client).await?;
        Ok(Self { manager })
    }

    pub async fn lpush(&self, key: &str, value: String) -> Result<()> {
        let mut conn = self.manager.clone();
        conn.lpush(key, value).await?;
        Ok(())
    }

    pub async fn zadd(&self, key: &str, member: &str, score: f64) -> Result<()> {
        let mut conn = self.manager.clone();
        conn.zadd(key, member, score).await?;
        Ok(())
    }

    pub async fn publish(&self, channel: &str, message: String) -> Result<()> {
        let mut conn = self.manager.clone();
        conn.publish(channel, message).await?;
        Ok(())
    }
}