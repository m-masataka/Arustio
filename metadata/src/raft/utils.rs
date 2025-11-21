pub const MOUNT_PREFIX: &[u8] = b"M:";
pub const PATH_PREFIX:  &[u8] = b"P:";
pub const ID_PREFIX:    &[u8] = b"I:";

/// Helper that turns a u64 into an 8-byte big-endian array
pub fn u64be_bytes(x: u64) -> [u8; 8] {
    x.to_be_bytes()
}

/// Build a path key that looks like `P:/foo/bar`
pub fn kv_key_path<S: AsRef<str>>(path: S) -> Vec<u8> {
    let s = path.as_ref();
    let mut v = Vec::with_capacity(2 + s.len());
    v.extend_from_slice(PATH_PREFIX);
    v.extend_from_slice(s.as_bytes());
    v
}

pub fn kv_key_mount_path<S: AsRef<str>>(path: S) -> Vec<u8> {
    let s = path.as_ref();
    let mut v = Vec::with_capacity(2 + s.len());
    v.extend_from_slice(MOUNT_PREFIX);
    v.extend_from_slice(s.as_bytes());
    v
}

pub fn kv_key_id(id: &str) -> Vec<u8> {
    let mut v = Vec::with_capacity(2 + id.len());
    v.extend_from_slice(ID_PREFIX);
    v.extend_from_slice(id.as_bytes());
    v
}
