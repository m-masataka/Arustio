use crate::config::UfsConfig;
use common::{Error, Result};
use object_store::ObjectStore;
use std::sync::Arc;

pub struct UnifiedStore;

impl UnifiedStore {
    /// Create an ObjectStore from configuration
    pub async fn from_config(config: UfsConfig) -> Result<Arc<dyn ObjectStore>> {
        match config {
            UfsConfig::S3 {
                bucket,
                region,
                access_key_id,
                secret_access_key,
                endpoint,
            } => {
                use object_store::aws::AmazonS3Builder;

                let mut builder = AmazonS3Builder::new()
                    .with_bucket_name(&bucket)
                    .with_region(&region);

                if let Some(key_id) = access_key_id {
                    builder = builder.with_access_key_id(&key_id);
                }

                if let Some(secret) = secret_access_key {
                    builder = builder.with_secret_access_key(&secret);
                }

                if let Some(ep) = &endpoint {
                    builder = builder.with_endpoint(ep);

                    // For MinIO and custom S3 endpoints
                    if ep.starts_with("http://") {
                        builder = builder.with_allow_http(true);
                    }

                    // Use path-style requests for MinIO compatibility
                    builder = builder.with_virtual_hosted_style_request(false);
                }

                let store = builder.build()
                    .map_err(|e| Error::Storage(format!("Failed to create S3 store: {}", e)))?;

                Ok(Arc::new(store))
            }

            UfsConfig::Gcs {
                bucket,
                service_account_path,
            } => {
                use object_store::gcp::GoogleCloudStorageBuilder;

                let mut builder = GoogleCloudStorageBuilder::new()
                    .with_bucket_name(&bucket);

                if let Some(sa_path) = service_account_path {
                    builder = builder.with_service_account_path(&sa_path);
                }

                let store = builder.build()
                    .map_err(|e| Error::Storage(format!("Failed to create GCS store: {}", e)))?;

                Ok(Arc::new(store))
            }

            UfsConfig::Azure {
                container,
                account,
                access_key,
            } => {
                use object_store::azure::MicrosoftAzureBuilder;

                let mut builder = MicrosoftAzureBuilder::new()
                    .with_container_name(&container)
                    .with_account(&account);

                if let Some(key) = access_key {
                    builder = builder.with_access_key(&key);
                }

                let store = builder.build()
                    .map_err(|e| Error::Storage(format!("Failed to create Azure store: {}", e)))?;

                Ok(Arc::new(store))
            }

            UfsConfig::Local { root_path } => {
                use object_store::local::LocalFileSystem;
                use std::path::PathBuf;

                let path = PathBuf::from(root_path);
                let store = LocalFileSystem::new_with_prefix(path)
                    .map_err(|e| Error::Storage(format!("Failed to create local store: {}", e)))?;

                Ok(Arc::new(store))
            }

            UfsConfig::Memory => {
                use object_store::memory::InMemory;
                Ok(Arc::new(InMemory::new()))
            }
        }
    }
}
