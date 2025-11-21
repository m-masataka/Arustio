use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// File type enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileType {
    File,
    Directory,
}

impl FileType {
    pub fn from_i32(v: i32) -> Option<Self> {
        match v {
            x if x == FileType::File as i32 => Some(FileType::File),
            x if x == FileType::Directory as i32 => Some(FileType::Directory),
            _ => None,
        }
    }
}

/// File metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMetadata {
    /// Unique file ID
    pub id: Uuid,

    /// File path
    pub path: String,

    /// File type
    pub file_type: FileType,

    /// File size in bytes
    pub size: u64,

    /// Creation time
    pub created_at: DateTime<Utc>,

    /// Last modification time
    pub modified_at: DateTime<Utc>,

    /// Parent directory ID (None for root)
    pub parent_id: Option<Uuid>,

    /// UFS path (actual object storage path)
    pub ufs_path: Option<String>,
}

impl FileMetadata {
    pub fn new_directory(path: String, parent_id: Option<Uuid>) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            path,
            file_type: FileType::Directory,
            size: 0,
            created_at: now,
            modified_at: now,
            parent_id,
            ufs_path: None,
        }
    }

    pub fn new_file(path: String, size: u64, parent_id: Option<Uuid>, ufs_path: String) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            path,
            file_type: FileType::File,
            size,
            created_at: now,
            modified_at: now,
            parent_id,
            ufs_path: Some(ufs_path),
        }
    }

    pub fn is_directory(&self) -> bool {
        self.file_type == FileType::Directory
    }

    pub fn is_file(&self) -> bool {
        self.file_type == FileType::File
    }
}

/// File status information (similar to stat)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileStatus {
    pub path: String,
    pub file_type: FileType,
    pub size: u64,
    pub modified_at: DateTime<Utc>,
}

impl From<&FileMetadata> for FileStatus {
    fn from(metadata: &FileMetadata) -> Self {
        Self {
            path: metadata.path.clone(),
            file_type: metadata.file_type,
            size: metadata.size,
            modified_at: metadata.modified_at,
        }
    }
}



use std::convert::TryFrom;
use crate::meta::FileMetadata as ProtoFileMetadata;
use prost_types::Timestamp;

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

impl From<FileMetadata> for ProtoFileMetadata {
    fn from(meta: FileMetadata) -> Self {
        ProtoFileMetadata {
            id: meta.id.to_string(),
            path: meta.path,
            file_type: match meta.file_type {
                FileType::File => FileType::File as i32,
                FileType::Directory => FileType::Directory as i32,
            },
            size: meta.size,
            created_at: Some(Timestamp {
                seconds: meta.created_at.timestamp(),
                nanos: meta.created_at.timestamp_subsec_nanos() as i32,
            }),
            modified_at: Some(Timestamp {
                seconds: meta.modified_at.timestamp(),
                nanos: meta.modified_at.timestamp_subsec_nanos() as i32,
            }),
            parent_id: opt_to_string(meta.parent_id.map(|pid| pid.to_string())),
            ufs_path: meta.ufs_path.unwrap_or_default(),
        }
    }
}

impl TryFrom<ProtoFileMetadata> for FileMetadata {
    type Error = String;

    fn try_from(proto: ProtoFileMetadata) -> Result<Self, Self::Error> {
        Ok(Self {
            id: Uuid::parse_str(&proto.id)
                .map_err(|e| format!("Invalid UUID in FileMetadata.id: {}", e))?,
            path: proto.path,
            file_type: match FileType::from_i32(proto.file_type) {
                Some(FileType::File) => FileType::File,
                Some(FileType::Directory) => FileType::Directory,
                None => return Err("Invalid file_type in FileMetadata".to_string()),
            },
            size: proto.size,
            created_at: {
                let ts = proto.created_at.ok_or_else(|| "Missing created_at in FileMetadata".to_string())?;
                DateTime::<Utc>::from_utc(
                    chrono::NaiveDateTime::from_timestamp_opt(ts.seconds, ts.nanos as u32)
                        .ok_or_else(|| "Invalid created_at timestamp in FileMetadata".to_string())?,
                    Utc,
                )
            },
            modified_at: {
                let ts = proto.modified_at.ok_or_else(|| "Missing modified_at in FileMetadata".to_string())?;
                DateTime::<Utc>::from_utc(
                    chrono::NaiveDateTime::from_timestamp_opt(ts.seconds, ts.nanos as u32)
                        .ok_or_else(|| "Invalid modified_at timestamp in FileMetadata".to_string())?,
                    Utc,
                )
            },
            parent_id: match string_to_opt(proto.parent_id) {
                Some(pid) => Some(Uuid::parse_str(&pid)
                    .map_err(|e| format!("Invalid UUID in FileMetadata.parent_id: {}", e))?),
                None => None,
            },
            ufs_path: if proto.ufs_path.is_empty() {
                None
            } else {
                Some(proto.ufs_path)
            },
        })
    }
}
