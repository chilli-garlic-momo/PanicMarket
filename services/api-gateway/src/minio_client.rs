use anyhow::Result;
use aws_config::Region;
use aws_sdk_s3::{
    config::{Credentials, SharedCredentialsProvider},
    Client, Config,
};
use aws_sdk_s3::primitives::ByteStream;
use tracing::info;

#[derive(Clone)]
pub struct MinioClient {
    client: Client,
    bucket: String,
}

impl MinioClient {
    pub async fn new(
        endpoint: &str,
        access_key: &str,
        secret_key: &str,
        bucket: &str,
    ) -> Result<Self> {
        let credentials = Credentials::new(
            access_key,
            secret_key,
            None,
            None,
            "static",
        );

        let config = Config::builder()
            .endpoint_url(endpoint)
            .credentials_provider(SharedCredentialsProvider::new(credentials))
            .region(Region::new("us-east-1"))
            .force_path_style(true)
            .build();

        let client = Client::from_conf(config);

        // Ensure bucket exists
        match client.head_bucket().bucket(bucket).send().await {
            Ok(_) => info!("MinIO bucket '{}' exists", bucket),
            Err(_) => {
                client.create_bucket()
                    .bucket(bucket)
                    .send()
                    .await?;
                info!("MinIO bucket '{}' created", bucket);
            }
        }

        Ok(Self {
            client,
            bucket: bucket.to_string(),
        })
    }

    pub async fn upload(&self, key: &str, data: Vec<u8>) -> Result<()> {
        let stream = ByteStream::from(data);
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(stream)
            .send()
            .await?;
        Ok(())
    }

    pub async fn download(&self, key: &str) -> Result<Vec<u8>> {
        let resp = self.client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await?;

        let data = resp.body.collect().await?;
        Ok(data.into_bytes().to_vec())
    }
}