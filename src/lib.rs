pub mod common;
pub mod client;
pub mod core;
pub mod metadata;
pub mod server;
pub mod ufs;
pub mod cache;
pub mod block;
pub mod cmd;

// Generated gRPC code
pub mod mount {
    tonic::include_proto!("arustio.mount");
}

pub mod file {
    tonic::include_proto!("arustio.file");
}

pub mod blockio {
    tonic::include_proto!("arustio.blockio");
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
