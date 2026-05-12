// Same as api-gateway/src/minio_client.rs - shared implementation
use anyhow::Result;
use aws_config::Region;
use aws_sdk_s3::{
    config::{Credentials, SharedCredentialsProvider},
    Client, Config,
};

#[derive(Clone)]
pub struct MinioClient {
    client: Client,
    bucket: String,
}

impl MinioClient {
    pub async fn new(endpoint: &str, access_key: &str, secret_key: &str, bucket: &str) -> Result<Self> {
        let credentials = Credentials::new(access_key, secret_key, None, None, "static");
        let config = Config::builder()
            .endpoint_url(endpoint)
            .credentials_provider(SharedCredentialsProvider::new(credentials))
            .region(Region::new("us-east-1"))
            .force_path_style(true)
            .build();
        Ok(Self { client: Client::from_conf(config), bucket: bucket.to_string() })
    }

    pub async fn download(&self, key: &str) -> Result<Vec<u8>> {
        let resp = self.client.get_object().bucket(&self.bucket).key(key).send().await?;
        let data = resp.body.collect().await?;
        Ok(data.into_bytes().to_vec())
    }
}