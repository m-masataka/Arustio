//! Additional filesystem operations and utilities

use common::Result;

/// Copy a file from one path to another
pub async fn copy(
    fs: &dyn super::FileSystem,
    src: &str,
    dst: &str,
) -> Result<()> {
    let data = fs.read(src).await?;
    fs.create(dst, data).await?;
    Ok(())
}

/// Move/rename a file or directory
/// Note: This is a simplified implementation that copies and deletes
pub async fn rename(
    fs: &dyn super::FileSystem,
    src: &str,
    dst: &str,
) -> Result<()> {
    let stat = fs.stat(src).await?;

    if stat.file_type == common::file_metadata::FileType::File {
        let data = fs.read(src).await?;
        fs.create(dst, data).await?;
        fs.remove_file(src).await?;
    } else {
        // For directories, this is more complex and would require recursive handling
        // For now, just return an error
        return Err(common::Error::Internal(
            "Directory rename not yet implemented".to_string()
        ));
    }

    Ok(())
}
