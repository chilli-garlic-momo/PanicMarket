use anyhow::Result;
use bollard::{
    Docker,
    container::{CreateContainerOptions, Config, StartContainerOptions, RemoveContainerOptions},
    models::HostConfig,
    network::CreateNetworkOptions,
};
use std::collections::HashMap;
use tracing::{info, warn};
use uuid::Uuid;

pub struct Deployer {
    docker: Docker,
    mode: String,
}

impl Deployer {
    pub async fn new(mode: &str) -> Result<Self> {
        let docker = Docker::connect_with_local_defaults()?;
        docker.ping().await?;

        // Ensure benchmark network exists
        let network_name = "benchmark-net";
        match docker.inspect_network(network_name, None::<bollard::network::InspectNetworkOptions<String>>).await {
            Ok(_) => {}
            Err(_) => {
                docker.create_network(CreateNetworkOptions {
                    name: network_name,
                    driver: "bridge",
                    ..Default::default()
                }).await?;
                info!("Created Docker network: {}", network_name);
            }
        }

        Ok(Self { docker, mode: mode.to_string() })
    }

    /// Deploy engine, returns (http_endpoint, container_id)
    pub async fn deploy(&self, image_ref: &str, test_id: Uuid) -> Result<(String, String)> {
        match self.mode.as_str() {
            "docker" => self.deploy_docker(image_ref, test_id).await,
            "kubernetes" => self.deploy_kubernetes(image_ref, test_id).await,
            _ => Err(anyhow::anyhow!("Unknown deployment mode: {}", self.mode)),
        }
    }

    async fn deploy_docker(&self, image_ref: &str, test_id: Uuid) -> Result<(String, String)> {
        let container_name = format!("engine-{}", test_id);

        // Pull image if needed
        info!("Pulling image: {}", image_ref);
        let mut pull_stream = self.docker.create_image(
            Some(bollard::image::CreateImageOptions {
                from_image: image_ref,
                ..Default::default()
            }),
            None,
            None,
        );

        use futures_util::StreamExt;
        while let Some(event) = pull_stream.next().await {
            match event {
                Ok(_) => {}
                Err(e) => {
                    // Image might already be local
                    warn!("Pull event error (may be OK): {}", e);
                    break;
                }
            }
        }

        // Create container with resource limits
        let mut env = vec![
            "RUST_LOG=info".to_string(),
        ];

        let host_config = HostConfig {
            network_mode: Some("benchmark-net".to_string()),
            nano_cpus: Some(4_000_000_000), // 4 CPUs
            memory: Some(4 * 1024 * 1024 * 1024), // 4 GB
            memory_swap: Some(4 * 1024 * 1024 * 1024), // No swap
            auto_remove: Some(false),
            ..Default::default()
        };

        let container = self.docker.create_container(
            Some(CreateContainerOptions {
                name: container_name.as_str(),
                platform: None,
            }),
            Config {
                image: Some(image_ref),
                env: Some(env.iter().map(String::as_str).collect()),
                host_config: Some(host_config),
                ..Default::default()
            },
        ).await?;

        let container_id = container.id.clone();
        info!("Created container {} ({})", container_name, container_id);

        // Start container
        self.docker.start_container(&container_id, None::<StartContainerOptions<String>>).await?;
        info!("Container started: {}", container_id);

        // Get container IP on benchmark-net
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        let inspect = self.docker.inspect_container(&container_id, None).await?;
        let ip = inspect
            .network_settings
            .as_ref()
            .and_then(|ns| ns.networks.as_ref())
            .and_then(|nets| nets.get("benchmark-net"))
            .and_then(|net| net.ip_address.as_ref())
            .filter(|ip| !ip.is_empty())
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Could not get container IP"))?;

        let endpoint = format!("http://{}:8080", ip);
        info!("Engine endpoint: {}", endpoint);

        Ok((endpoint, container_id))
    }

    async fn deploy_kubernetes(&self, image_ref: &str, test_id: Uuid) -> Result<(String, String)> {
        // Phase 2: Kubernetes deployment via kubectl/k8s client
        todo!("Kubernetes deployment - Phase 2")
    }

    pub async fn cleanup(&self, container_id: &str) -> Result<()> {
        // Stop container
        let _ = self.docker.stop_container(container_id, None).await;

        // Remove container
        self.docker.remove_container(
            container_id,
            Some(RemoveContainerOptions {
                force: true,
                ..Default::default()
            }),
        ).await?;

        info!("Cleaned up container {}", container_id);
        Ok(())
    }
}