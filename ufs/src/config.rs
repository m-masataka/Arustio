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
    Local {
        root_path: String,
    },

    /// In-memory storage (for testing)
    Memory,
}
