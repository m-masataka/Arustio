use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Path not found: {0}")]
    PathNotFound(String),

    #[error("Path already exists: {0}")]
    PathAlreadyExists(String),

    #[error("Not a directory: {0}")]
    NotADirectory(String),

    #[error("Not a file: {0}")]
    NotAFile(String),

    #[error("Invalid path: {0}")]
    InvalidPath(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Metadata error: {0}")]
    Metadata(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

pub type Result<T> = std::result::Result<T, Error>;
