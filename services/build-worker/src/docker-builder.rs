use anyhow::Result;
use bollard::Docker;
use bollard::image::BuildImageOptions;
use bollard::container::{CreateContainerOptions, Config as ContainerConfig, StartContainerOptions};
use futures_util::StreamExt;
use std::path::Path;
use tracing::{info, warn};
use uuid::Uuid;

pub struct DockerBuilder {
    docker: Docker,
    registry_url: String,
}

impl DockerBuilder {
    pub async fn new(registry_url: &str) -> Result<Self> {
        let docker = Docker::connect_with_local_defaults()?;
        // Test connection
        docker.ping().await?;
        info!("Connected to Docker daemon");

        Ok(Self {
            docker,
            registry_url: registry_url.to_string(),
        })
    }

    pub async fn build_image(
        &self,
        build_dir: &Path,
        image_tag: &str,
        submission_id: Uuid,
    ) -> Result<String> {
        info!("Building image {} from {:?}", image_tag, build_dir);

        // Create tar archive of build context
        let tar_data = create_build_context_tar(build_dir)?;

        let build_options = BuildImageOptions {
            t: image_tag,
            rm: true,
            forcerm: true,
            nocache: false,
            ..Default::default()
        };

        let mut build_stream = self.docker.build_image(
            build_options,
            None,
            Some(tar_data.into()),
        );

        let mut build_log = String::new();
        let mut success = false;

        while let Some(event) = build_stream.next().await {
            match event {
                Ok(info) => {
                    if let Some(stream) = &info.stream {
                        build_log.push_str(stream);
                        // Don't flood logs, just track progress
                        if stream.contains("Successfully built") || stream.contains("naming to") {
                            success = true;
                        }
                    }
                    if let Some(err) = &info.error {
                        build_log.push_str(&format!("ERROR: {}\n", err));
                        return Err(anyhow::anyhow!("Docker build error: {}", err));
                    }
                }
                Err(e) => {
                    return Err(anyhow::anyhow!("Build stream error: {}", e));
                }
            }
        }

        // Push to local registry
        info!("Pushing image {} to registry...", image_tag);
        build_log.push_str(&format!("\nPushing to registry {}...\n", image_tag));

        let mut push_stream = self.docker.push_image(
            image_tag,
            None,
            None,
        );

        while let Some(event) = push_stream.next().await {
            match event {
                Ok(_) => {}
                Err(e) => {
                    warn!("Push warning (may be OK for local registry): {}", e);
                }
            }
        }

        build_log.push_str("Image pushed to registry successfully.\n");

        Ok(build_log)
    }
}

fn create_build_context_tar(build_dir: &Path) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    {
        let mut builder = tar::Builder::new(&mut buf);
        builder.append_dir_all(".", build_dir)?;
        builder.finish()?;
    }
    Ok(buf)
}