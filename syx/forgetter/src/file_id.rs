//! A segment's identity: `{id:020}.log` in `Forgetter`'s directory.

use std::ffi::OsStr;
use std::fmt;
use std::path::{
    Path,
    PathBuf,
};
use std::sync::atomic::{
    AtomicU64,
    Ordering,
};

/// The source of fresh ids for every `next` call in this process.
static ID: AtomicU64 = AtomicU64::new(0);

/// Advances `ID` so it never hands out an id already used by `id`.
/// Safe to call with any id, in any order, concurrently.
pub(super) fn seed(id: FileId) {
    ID.fetch_max(id.0 + 1, Ordering::Relaxed);
}

/// Claims a fresh, unique id.
pub(super) fn next() -> FileId {
    FileId(ID.fetch_add(1, Ordering::Relaxed))
}

/// A segment's identity, and the `{id:020}.log` name it's stored under.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct FileId(u64);

impl fmt::Display for FileId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Exactly wide enough for u64::MAX (20 digits).
        write!(f, "{:020}", self.0)
    }
}

pub(super) fn path(dir: &Path, id: FileId) -> PathBuf {
    dir.join(format!("{id}.log"))
}

pub(super) fn parse(file_name: &OsStr) -> Option<FileId> {
    file_name.to_str()?.strip_suffix(".log")?.parse().ok().map(FileId)
}
