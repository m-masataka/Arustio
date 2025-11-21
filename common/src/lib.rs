pub mod error;
pub mod file_metadata;
pub mod path;
pub mod config;
pub mod raft_client;

pub use error::{Error, Result};
pub use file::*;
pub use path::*;
pub use config::*;

// Generated gRPC code
pub mod mount {
    tonic::include_proto!("arustio.mount");
}

pub mod file {
    tonic::include_proto!("arustio.file");
}

pub mod meta {
    tonic::include_proto!("arustio.meta");
}

pub mod snapshot {
    tonic::include_proto!("arustio.snapshot");
}

pub mod raftio {
    tonic::include_proto!("arustio.raftio");
}
