use common::{Error, Result};
use tokio::sync::{mpsc, oneshot};

/// Request sent from readers to the Raft core loop when a linearizable read is needed.
pub struct LinearizableReadRequest {
    pub completion: oneshot::Sender<Result<()>>,
}

#[derive(Clone)]
pub struct LinearizableReadHandle {
    tx: mpsc::Sender<LinearizableReadRequest>,
}

impl LinearizableReadHandle {
    pub fn new(tx: mpsc::Sender<LinearizableReadRequest>) -> Self {
        Self { tx }
    }

    /// Wait until the local state machine is linearizable with respect to the leader.
    pub async fn wait(&self) -> Result<()> {
        let (completion, rx) = oneshot::channel();
        self.tx
            .send(LinearizableReadRequest { completion })
            .await
            .map_err(|_| Error::Internal("Raft node not ready for linearizable read".into()))?;
        rx.await
            .map_err(|_| Error::Internal("Raft linearizable read response dropped".into()))?
    }
}
