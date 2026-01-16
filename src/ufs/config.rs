use serde::{Deserialize, Serialize};

/// UFS backend type
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum UfsConfig {
    /// Amazon S3
    S3 {
        bucket: String,
        region: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        access_key_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        secret_access_key: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        endpoint: Option<String>,
    },

    /// Google Cloud Storage
    Gcs {
        bucket: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        service_account_path: Option<String>,
    },

    /// Azure Blob Storage
    Azure {
        container: String,
        account: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        access_key: Option<String>,
    },

    /// Local filesystem (for testing)
    Local { root_path: String },

    /// In-memory storage (for testing)
    Memory,
}

use crate::meta::UfsConfig as ProtoUfsConfig;
use crate::meta::ufs_config::Config;
use crate::meta::{AzureConfig, GcsConfig, LocalConfig, MemoryConfig, S3Config};
use std::convert::TryFrom;

fn opt_to_string(v: Option<String>) -> String {
    v.unwrap_or_default()
}

fn string_to_opt(s: String) -> Option<String> {
    if s.is_empty() { None } else { Some(s) }
}

impl From<UfsConfig> for ProtoUfsConfig {
    fn from(cfg: UfsConfig) -> Self {
        let config = match cfg {
            UfsConfig::S3 {
                access_key_id,
                secret_access_key,
                region,
                bucket,
                endpoint,
            } => Config::S3(S3Config {
                access_key_id: opt_to_string(access_key_id),
                secret_access_key: opt_to_string(secret_access_key),
                region,
                bucket,
                endpoint: opt_to_string(endpoint),
            }),
            UfsConfig::Gcs {
                bucket,
                service_account_path,
            } => Config::Gcs(GcsConfig {
                bucket,
                service_account_path: opt_to_string(service_account_path),
            }),
            UfsConfig::Azure {
                container,
                account,
                access_key,
            } => Config::Azure(AzureConfig {
                container,
                account,
                access_key: opt_to_string(access_key),
            }),
            UfsConfig::Local { root_path } => Config::Local(LocalConfig { root_path }),
            UfsConfig::Memory => Config::Memory(MemoryConfig {}),
        };
        ProtoUfsConfig {
            config: Some(config),
        }
    }
}

impl TryFrom<ProtoUfsConfig> for UfsConfig {
    type Error = String;

    fn try_from(cfg: ProtoUfsConfig) -> Result<Self, Self::Error> {
        let config = cfg
            .config
            .ok_or_else(|| "UfsConfig.config is None (oneof not set)".to_string())?;

        let res = match config {
            Config::S3(s3) => UfsConfig::S3 {
                bucket: s3.bucket,
                region: s3.region,
                access_key_id: string_to_opt(s3.access_key_id),
                secret_access_key: string_to_opt(s3.secret_access_key),
                endpoint: string_to_opt(s3.endpoint),
            },

            Config::Gcs(gcs) => UfsConfig::Gcs {
                bucket: gcs.bucket,
                service_account_path: string_to_opt(gcs.service_account_path),
            },

            Config::Azure(az) => UfsConfig::Azure {
                container: az.container,
                account: az.account,
                access_key: string_to_opt(az.access_key),
            },

            Config::Local(local) => UfsConfig::Local {
                root_path: local.root_path,
            },

            Config::Memory(_mem) => UfsConfig::Memory,
        };

        Ok(res)
    }
}
