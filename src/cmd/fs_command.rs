use clap::Subcommand;

#[derive(Subcommand)]
pub enum FsCommands {
    /// Mount an under file system to a path
    Mount {
        /// Virtual path to mount to (e.g., /dev/dev1)
        #[arg(value_name = "PATH")]
        path: String,

        /// URI of the under file system (e.g., s3a://bucket1/dir1/dir2)
        #[arg(value_name = "URI")]
        uri: String,

        /// Optional mount options (e.g., readonly, write-policy)
        #[arg(short, long, value_name = "KEY=VALUE")]
        option: Vec<String>,
    },

    /// Unmount a path
    Unmount {
        /// Virtual path to unmount (e.g., /dev/dev1)
        #[arg(value_name = "PATH")]
        path: String,
    },

    /// List all mount points
    List,

    /// List files and directories (like ls)
    Ls {
        /// Path to list
        #[arg(value_name = "PATH", default_value = "/")]
        path: String,

        /// Long format (detailed information)
        #[arg(short, long)]
        long: bool,
    },

    /// Stat a file or directory
    Stat {
        /// Path to stat
        #[arg(value_name = "PATH")]
        path: String,
    },

    /// Create a directory
    Mkdir {
        /// Path to create
        #[arg(value_name = "PATH")]
        path: String,
    },

    /// Copy file from local filesystem to Arustio
    CopyFromLocal {
        /// Local file path
        #[arg(value_name = "LOCAL_PATH")]
        local_path: String,

        /// Arustio destination path
        #[arg(value_name = "REMOTE_PATH")]
        remote_path: String,
    },

    /// Copy file from Arustio to local filesystem
    CopyToLocal {
        /// Arustio source path
        #[arg(value_name = "REMOTE_PATH")]
        remote_path: String,

        /// Local destination path
        #[arg(value_name = "LOCAL_PATH")]
        local_path: String,
    },

    /// Set path configuration (e.g., writetype=NO_CACHE)
    SetConf {
        /// Path to set configuration on
        #[arg(value_name = "PATH")]
        path: String,

        /// Key=Value pairs
        #[arg(value_name = "KEY=VALUE")]
        conf: Vec<String>,
    },
}
