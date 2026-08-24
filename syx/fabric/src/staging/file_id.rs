//! A segment's identity: `{id:020}.log` in `Staging`'s directory. A
//! segment is created empty, then appended to sequentially while it's
//! the active one; once rotated out it never changes again until
//! `finish` deletes it. `segment` itself doesn't need this (see its own
//! module doc); it only matters to `staging`'s own bookkeeping --
//! `Committer::file_id` and `pending`'s keys.

use std::ffi::OsStr;
use std::fmt;
use std::path::{
    Path,
    PathBuf,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct FileId(u64);

impl FileId {
    pub(super) const FIRST: FileId = FileId(0);

    pub(super) fn next(self) -> FileId {
        FileId(self.0 + 1)
    }
}

impl fmt::Display for FileId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:020}", self.0)
    }
}

pub(super) fn path(dir: &Path, id: FileId) -> PathBuf {
    dir.join(format!("{id}.log"))
}

pub(super) fn parse(file_name: &OsStr) -> Option<FileId> {
    file_name.to_str()?.strip_suffix(".log")?.parse().ok().map(FileId)
}
