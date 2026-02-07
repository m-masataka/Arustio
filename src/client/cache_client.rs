use tonic::async_trait;

use crate::common::Result;
use bytes::Bytes;
use uuid::Uuid;

#[async_trait]
pub trait CacheClient: Send + Sync {
    async fn connection_pool(&self) -> Result<()>;
    async fn read_block(&self, file_id: Uuid, index: u64) -> Result<Bytes>;
    async fn write_block(&self, file_id: Uuid, index: u64, data: Bytes) -> Result<()>;
}