use std::convert::TryFrom;
use common::meta::UfsConfig as ProtoUfsConfig;
use crate::config::{
    UfsConfig as DomainUfsConfig,
};
use common::meta::ufs_config::Config;
use common::meta::{
    GcsConfig,
    S3Config,
    AzureConfig,
    LocalConfig,
    MemoryConfig,
};

fn opt_to_string(v: Option<String>) -> String {
    v.unwrap_or_default()
}

fn string_to_opt(s: String) -> Option<String> {
    if s.is_empty() {
        None
    } else {
        Some(s)
    }
}

impl From<DomainUfsConfig> for ProtoUfsConfig {
    fn from(cfg: DomainUfsConfig) -> Self {
        let config = match cfg {
            DomainUfsConfig::S3 {
                access_key_id,
                secret_access_key,
                region,
                bucket,
                endpoint,
            } => {
                Config::S3(S3Config {
                    access_key_id: opt_to_string(access_key_id),
                    secret_access_key: opt_to_string(secret_access_key),
                    region,
                    bucket,
                    endpoint: opt_to_string(endpoint),
                })
            },
            DomainUfsConfig::Gcs {
                bucket,
                service_account_path,
            } => {
                Config::Gcs(GcsConfig {
                    bucket,
                    service_account_path: opt_to_string(service_account_path),
                })
            },
            DomainUfsConfig::Azure {
                container,
                account,
                access_key,
            } => {
                Config::Azure(AzureConfig {
                    container,
                    account,
                    access_key: opt_to_string(access_key),
                })
            },
            DomainUfsConfig::Local { root_path } => {
                Config::Local(LocalConfig { root_path })
            },
            DomainUfsConfig::Memory => Config::Memory(MemoryConfig {}),
        };
        ProtoUfsConfig { config: Some(config) }
    }
}

impl TryFrom<ProtoUfsConfig> for DomainUfsConfig {
    type Error = String;

    fn try_from(cfg: ProtoUfsConfig) -> Result<Self, Self::Error> {

        let config = cfg
            .config
            .ok_or_else(|| "UfsConfig.config is None (oneof not set)".to_string())?;

        let res = match config {
            Config::S3(s3) => DomainUfsConfig::S3 {
                bucket: s3.bucket,
                region: s3.region,
                access_key_id: string_to_opt(s3.access_key_id),
                secret_access_key: string_to_opt(s3.secret_access_key),
                endpoint: string_to_opt(s3.endpoint),
            },

            Config::Gcs(gcs) => DomainUfsConfig::Gcs {
                bucket: gcs.bucket,
                service_account_path: string_to_opt(gcs.service_account_path),
            },

            Config::Azure(az) => DomainUfsConfig::Azure {
                container: az.container,
                account: az.account,
                access_key: string_to_opt(az.access_key),
            },

            Config::Local(local) => DomainUfsConfig::Local {
                root_path: local.root_path,
            },

            Config::Memory(_mem) => DomainUfsConfig::Memory,
        };

        Ok(res)
    }
}