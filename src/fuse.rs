use crate::{
    client::virtual_file_system::VirtualFileSystem,
    common::{Error, normalize_path},
    core::file_metadata::FileMetadata,
    core::file_system::FileSystem,
    ufs::under_file_system::Ufs,
};
use bytes::{Bytes, BytesMut};
use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyCreate, ReplyData, ReplyDirectory,
    ReplyEntry, ReplyOpen, ReplyWrite, Request, TimeOrNow,
};
use futures::{StreamExt, stream};
use libc::{EINVAL, EIO, ENOENT};
use std::{
    collections::HashMap,
    ffi::OsStr,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::runtime::Runtime;

const TTL: Duration = Duration::from_secs(1);

#[derive(Clone)]
struct WriteHandle {
    path: String,
    data: BytesMut,
    dirty: bool,
}

#[derive(Clone)]
struct ReadHandle {
    path: String,
    ufs: Arc<Ufs>,
    relative_path: String,
    metadata: FileMetadata,
    cache_enabled: bool,
}

enum Handle {
    Read(ReadHandle),
    Write(WriteHandle),
}

pub struct ArustioFuse {
    vfs: VirtualFileSystem,
    rt: Runtime,
    inode_map: RwLock<HashMap<String, u64>>,
    reverse_map: RwLock<HashMap<u64, String>>,
    next_ino: AtomicU64,
    handles: Mutex<HashMap<u64, Handle>>,
    next_fh: AtomicU64,
}

impl ArustioFuse {
    pub fn new(vfs: VirtualFileSystem) -> Self {
        let rt = Runtime::new().expect("tokio runtime");
        let mut inode_map = HashMap::new();
        let mut reverse_map = HashMap::new();
        inode_map.insert("/".to_string(), 1);
        reverse_map.insert(1, "/".to_string());
        Self {
            vfs,
            rt,
            inode_map: RwLock::new(inode_map),
            reverse_map: RwLock::new(reverse_map),
            next_ino: AtomicU64::new(2),
            handles: Mutex::new(HashMap::new()),
            next_fh: AtomicU64::new(1),
        }
    }

    pub fn mount(self, mountpoint: &str) -> anyhow::Result<()> {
        let options = [
            MountOption::FSName("arustio".to_string()),
            MountOption::AutoUnmount,
            MountOption::DefaultPermissions,
        ];
        fuser::mount2(self, mountpoint, &options)?;
        Ok(())
    }

    fn ensure_inode(&self, path: &str) -> u64 {
        if let Some(ino) = self.inode_map.read().unwrap().get(path) {
            return *ino;
        }
        let mut map = self.inode_map.write().unwrap();
        if let Some(ino) = map.get(path) {
            return *ino;
        }
        let ino = self.next_ino.fetch_add(1, Ordering::Relaxed);
        map.insert(path.to_string(), ino);
        self.reverse_map
            .write()
            .unwrap()
            .insert(ino, path.to_string());
        ino
    }

    fn path_for_ino(&self, ino: u64) -> Option<String> {
        self.reverse_map.read().unwrap().get(&ino).cloned()
    }

    fn file_attr(&self, ino: u64, meta: &FileMetadata) -> FileAttr {
        let kind = if meta.is_directory() {
            FileType::Directory
        } else {
            FileType::RegularFile
        };
        let perm = if meta.is_directory() { 0o755 } else { 0o644 };
        let mtime = to_system_time(meta.modified_at.timestamp());
        let ctime = to_system_time(meta.created_at.timestamp());
        FileAttr {
            ino,
            size: meta.size,
            blocks: 1,
            atime: mtime,
            mtime,
            ctime,
            crtime: ctime,
            kind,
            perm,
            nlink: if meta.is_directory() { 2 } else { 1 },
            uid: unsafe { libc::geteuid() },
            gid: unsafe { libc::getegid() },
            rdev: 0,
            blksize: 512,
            flags: 0,
        }
    }

    fn read_all(&self, path: &str) -> Result<Bytes, Error> {
        self.rt.block_on(async {
            let (_meta, mut stream) = self.vfs.read(path).await?;
            let mut buf = BytesMut::new();
            while let Some(chunk) = stream.next().await {
                let bytes = chunk?;
                buf.extend_from_slice(&bytes);
            }
            Ok(Bytes::from(buf))
        })
    }

    fn write_all(&self, path: &str, data: Bytes) -> Result<(), Error> {
        self.rt.block_on(async {
            let stream = stream::once(async move { Ok(data) }).boxed();
            self.vfs.write(path, stream).await
        })
    }
}

impl Filesystem for ArustioFuse {
    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        let Some(path) = self.path_for_ino(ino) else {
            reply.error(ENOENT);
            return;
        };
        let res = self.rt.block_on(async { self.vfs.stat(&path).await });
        match res {
            Ok(meta) => reply.attr(&TTL, &self.file_attr(ino, &meta)),
            Err(Error::PathNotFound(_)) => reply.error(ENOENT),
            Err(_) => reply.error(EIO),
        }
    }

    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let Some(parent_path) = self.path_for_ino(parent) else {
            reply.error(ENOENT);
            return;
        };
        let name = name.to_string_lossy();
        let child_path = join_child_path(&parent_path, &name);
        let Ok(child_path) = normalize_path(&child_path) else {
            reply.error(ENOENT);
            return;
        };
        let res = self.rt.block_on(async { self.vfs.stat(&child_path).await });
        match res {
            Ok(meta) => {
                let ino = self.ensure_inode(&child_path);
                reply.entry(&TTL, &self.file_attr(ino, &meta), 0);
            }
            Err(Error::PathNotFound(_)) => reply.error(ENOENT),
            Err(_) => reply.error(EIO),
        }
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let Some(path) = self.path_for_ino(ino) else {
            reply.error(ENOENT);
            return;
        };
        let res = self.rt.block_on(async { self.vfs.list(&path).await });
        let entries = match res {
            Ok(list) => list,
            Err(Error::PathNotFound(_)) => {
                reply.error(ENOENT);
                return;
            }
            Err(_) => {
                reply.error(EIO);
                return;
            }
        };

        let mut all = Vec::with_capacity(entries.len() + 2);
        all.push((ino, FileType::Directory, ".".to_string()));
        all.push((ino, FileType::Directory, "..".to_string()));
        for meta in entries {
            let child_ino = self.ensure_inode(&meta.path);
            let name = meta.path.rsplit('/').next().unwrap_or("").to_string();
            let kind = if meta.is_directory() {
                FileType::Directory
            } else {
                FileType::RegularFile
            };
            all.push((child_ino, kind, name));
        }

        let start = if offset < 0 { 0 } else { offset as usize };
        for (i, (child_ino, kind, name)) in all.into_iter().enumerate().skip(start) {
            let next_offset = (i + 1) as i64;
            if reply.add(child_ino, next_offset, kind, name) {
                break;
            }
        }
        reply.ok();
    }

    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let Some(parent_path) = self.path_for_ino(parent) else {
            reply.error(ENOENT);
            return;
        };
        let name = name.to_string_lossy();
        let path = join_child_path(&parent_path, &name);
        let Ok(path) = normalize_path(&path) else {
            reply.error(ENOENT);
            return;
        };
        let res = self.rt.block_on(async { self.vfs.mkdir(&path).await });
        match res {
            Ok(()) => {
                let meta = self
                    .rt
                    .block_on(async { self.vfs.stat(&path).await })
                    .map_err(|_| EIO);
                match meta {
                    Ok(meta) => {
                        let ino = self.ensure_inode(&path);
                        reply.entry(&TTL, &self.file_attr(ino, &meta), 0);
                    }
                    Err(e) => reply.error(e),
                }
            }
            Err(_) => reply.error(EIO),
        }
    }

    fn create(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        let Some(parent_path) = self.path_for_ino(parent) else {
            reply.error(ENOENT);
            return;
        };
        let name = name.to_string_lossy();
        let path = join_child_path(&parent_path, &name);
        let Ok(path) = normalize_path(&path) else {
            reply.error(ENOENT);
            return;
        };

        let res = self.write_all(&path, Bytes::new());
        match res {
            Ok(()) => {
                let meta = self
                    .rt
                    .block_on(async { self.vfs.stat(&path).await })
                    .map_err(|_| EIO);
                match meta {
                    Ok(meta) => {
                        let ino = self.ensure_inode(&path);
                        let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
                        self.handles.lock().unwrap().insert(
                            fh,
                            Handle::Write(WriteHandle {
                                path,
                                data: BytesMut::new(),
                                dirty: false,
                            }),
                        );
                        reply.created(&TTL, &self.file_attr(ino, &meta), 0, fh, 0);
                    }
                    Err(e) => reply.error(e),
                }
            }
            Err(_) => reply.error(EIO),
        }
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        let Some(path) = self.path_for_ino(ino) else {
            reply.error(ENOENT);
            return;
        };
        if is_write(flags) {
            let data = self.read_all(&path).unwrap_or_default();
            let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
            self.handles.lock().unwrap().insert(
                fh,
                Handle::Write(WriteHandle {
                    path,
                    data: BytesMut::from(&data[..]),
                    dirty: false,
                }),
            );
            reply.opened(fh, 0);
        } else {
            let res = self
                .rt
                .block_on(async { self.vfs.prepare_read_ctx(&path).await });
            match res {
                Ok(ctx) => {
                    let fh = self.next_fh.fetch_add(1, Ordering::Relaxed);
                    self.handles.lock().unwrap().insert(
                        fh,
                        Handle::Read(ReadHandle {
                            path,
                            ufs: ctx.ufs,
                            relative_path: ctx.relative_path,
                            metadata: ctx.metadata,
                            cache_enabled: ctx.cache_enabled,
                        }),
                    );
                    reply.opened(fh, 0);
                }
                Err(Error::PathNotFound(_)) => reply.error(ENOENT),
                Err(Error::NotAFile(_)) => reply.error(EINVAL),
                Err(_) => reply.error(EIO),
            }
        }
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        if offset < 0 {
            reply.error(EINVAL);
            return;
        }
        if fh != 0 {
            let read_handle = {
                let handles = self.handles.lock().unwrap();
                match handles.get(&fh) {
                    Some(Handle::Read(handle)) => Some(handle.clone()),
                    Some(Handle::Write(handle)) => {
                        let offset = offset as usize;
                        if offset >= handle.data.len() {
                            reply.data(&[]);
                            return;
                        }
                        let end = std::cmp::min(offset + size as usize, handle.data.len());
                        reply.data(&handle.data[offset..end]);
                        return;
                    }
                    None => None,
                }
            };
            if let Some(handle) = read_handle {
                let res = self.rt.block_on(async {
                    self.vfs
                        .read_range_with_ctx(
                            handle.ufs,
                            &handle.relative_path,
                            &handle.metadata,
                            offset as u64,
                            size as u64,
                            handle.cache_enabled,
                        )
                        .await
                });
                match res {
                    Ok(data) => reply.data(&data),
                    Err(Error::PathNotFound(_)) => reply.error(ENOENT),
                    Err(_) => reply.error(EIO),
                }
                return;
            }
        }

        let Some(path) = self.path_for_ino(ino) else {
            reply.error(ENOENT);
            return;
        };
        let res = self
            .rt
            .block_on(async { self.vfs.read_range(&path, offset as u64, size as u64).await });
        match res {
            Ok(data) => reply.data(&data),
            Err(Error::PathNotFound(_)) => reply.error(ENOENT),
            Err(_) => reply.error(EIO),
        }
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        let mut handles = self.handles.lock().unwrap();
        let Some(handle) = handles.get_mut(&fh) else {
            reply.error(ENOENT);
            return;
        };
        let Handle::Write(handle) = handle else {
            reply.error(EINVAL);
            return;
        };
        if offset < 0 {
            reply.error(EINVAL);
            return;
        }
        let offset = offset as usize;
        if handle.data.len() < offset + data.len() {
            handle.data.resize(offset + data.len(), 0);
        }
        handle.data[offset..offset + data.len()].copy_from_slice(data);
        handle.dirty = true;
        reply.written(data.len() as u32);
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<TimeOrNow>,
        _mtime: Option<TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let Some(path) = self.path_for_ino(ino) else {
            reply.error(ENOENT);
            return;
        };

        if let Some(new_size) = size {
            let res = self.read_all(&path).and_then(|data| {
                let mut buf = BytesMut::from(&data[..]);
                let new_size = new_size as usize;
                if buf.len() > new_size {
                    buf.truncate(new_size);
                } else if buf.len() < new_size {
                    buf.resize(new_size, 0);
                }
                self.write_all(&path, Bytes::from(buf))
            });
            if let Err(e) = res {
                match e {
                    Error::PathNotFound(_) => {
                        reply.error(ENOENT);
                        return;
                    }
                    _ => {
                        reply.error(EIO);
                        return;
                    }
                }
            }
        }

        let res = self.rt.block_on(async { self.vfs.stat(&path).await });
        match res {
            Ok(meta) => reply.attr(&TTL, &self.file_attr(ino, &meta)),
            Err(Error::PathNotFound(_)) => reply.error(ENOENT),
            Err(_) => reply.error(EIO),
        }
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        let handle = self.handles.lock().unwrap().remove(&fh);
        if let Some(handle) = handle {
            match handle {
                Handle::Write(handle) => {
                    if handle.dirty {
                        if self
                            .write_all(&handle.path, Bytes::from(handle.data))
                            .is_err()
                        {
                            reply.error(EIO);
                            return;
                        }
                    }
                }
                Handle::Read(_) => {}
            }
        }
        reply.ok();
    }
}

fn is_write(flags: i32) -> bool {
    (flags & libc::O_WRONLY) != 0 || (flags & libc::O_RDWR) != 0
}

fn join_child_path(parent: &str, name: &str) -> String {
    if parent == "/" {
        format!("/{}", name)
    } else {
        format!("{}/{}", parent, name)
    }
}

fn to_system_time(secs: i64) -> SystemTime {
    if secs <= 0 {
        UNIX_EPOCH
    } else {
        UNIX_EPOCH + Duration::from_secs(secs as u64)
    }
}
