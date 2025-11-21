use crate::error::{Error, Result};

/// Normalize a path to ensure it starts with / and doesn't end with /
pub fn normalize_path(path: &str) -> Result<String> {
    if path.is_empty() {
        return Err(Error::InvalidPath("Path cannot be empty".to_string()));
    }

    let mut normalized = path.to_string();

    // Ensure path starts with /
    if !normalized.starts_with('/') {
        normalized.insert(0, '/');
    }

    // Remove trailing slash unless it's the root
    if normalized.len() > 1 && normalized.ends_with('/') {
        normalized.pop();
    }

    // Remove duplicate slashes
    while normalized.contains("//") {
        normalized = normalized.replace("//", "/");
    }

    Ok(normalized)
}

/// Get the parent path
pub fn parent_path(path: &str) -> Option<String> {
    let normalized = normalize_path(path).ok()?;

    if normalized == "/" {
        return None;
    }

    let parts: Vec<&str> = normalized.rsplitn(2, '/').collect();
    if parts.len() == 2 {
        let parent = parts[1];
        if parent.is_empty() {
            Some("/".to_string())
        } else {
            Some(parent.to_string())
        }
    } else {
        Some("/".to_string())
    }
}

/// Get the file name from a path
pub fn file_name(path: &str) -> Option<String> {
    let normalized = normalize_path(path).ok()?;

    if normalized == "/" {
        return Some("/".to_string());
    }

    normalized.rsplit('/').next().map(|s| s.to_string())
}

/// Join two paths
pub fn join_path(base: &str, path: &str) -> Result<String> {
    let normalized_base = normalize_path(base)?;
    let normalized_path = normalize_path(path)?;

    if normalized_path.starts_with('/') {
        // Absolute path, return as is
        Ok(normalized_path)
    } else {
        // Relative path, join with base
        let joined = if normalized_base == "/" {
            format!("/{}", normalized_path)
        } else {
            format!("{}/{}", normalized_base, normalized_path)
        };
        normalize_path(&joined)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_path() {
        assert_eq!(normalize_path("/foo/bar").unwrap(), "/foo/bar");
        assert_eq!(normalize_path("foo/bar").unwrap(), "/foo/bar");
        assert_eq!(normalize_path("/foo/bar/").unwrap(), "/foo/bar");
        assert_eq!(normalize_path("/").unwrap(), "/");
        assert_eq!(normalize_path("//foo//bar//").unwrap(), "/foo/bar");
    }

    #[test]
    fn test_parent_path() {
        assert_eq!(parent_path("/foo/bar").unwrap(), "/foo");
        assert_eq!(parent_path("/foo").unwrap(), "/");
        assert_eq!(parent_path("/"), None);
    }

    #[test]
    fn test_file_name() {
        assert_eq!(file_name("/foo/bar").unwrap(), "bar");
        assert_eq!(file_name("/foo").unwrap(), "foo");
        assert_eq!(file_name("/").unwrap(), "/");
    }

    #[test]
    fn test_join_path() {
        assert_eq!(join_path("/foo", "bar").unwrap(), "/foo/bar");
        assert_eq!(join_path("/foo", "/bar").unwrap(), "/bar");
        assert_eq!(join_path("/", "foo").unwrap(), "/foo");
    }
}
