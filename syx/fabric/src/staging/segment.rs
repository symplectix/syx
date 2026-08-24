//! A segment's on-disk file and in-memory view.

use std::io;
use std::io::Write as _;
use std::path::PathBuf;
use std::sync::{
    Arc,
    OnceLock,
};

use bytes::Bytes;
use tokio::task;

/// A segment's open file, whether it's the one currently being appended
/// to or one already rotated out. Doesn't carry its own `FileId`: that's
/// tracked separately by whoever cares which segment this is (see
/// `staging`'s `Committer::id`, and `pending`'s own `FileId` keys) --
/// nothing in this module ever needs to read it back.
#[derive(Clone)]
pub(super) struct Segment {
    /// This segment's open file. `mmap` itself, once sealing calls for
    /// it, maps this same file. While this is the active segment, it's
    /// also the one handle `Committer` appends through.
    file: File,
    /// A read-only view of this whole segment. Empty for the active
    /// segment, which is never memory-mapped while still being
    /// written; filled in once the segment is sealed into `pending`.
    mmap: Mmap,
}

/// A segment's open file. Wraps `std::fs::File` behind position-
/// independent operations only: `read_at` for concurrent reads, `append`
/// for the one writer, and `mmap` for a one-time whole-file mapping.
#[derive(Clone)]
struct File(Arc<std::fs::File>);

/// A segment's whole-file mmap, established at most once and shared by
/// every clone of the `Segment` it came from.
#[derive(Clone, Default)]
struct Mmap(Arc<OnceLock<Bytes>>);

impl Segment {
    fn new(file: File) -> Segment {
        Segment { file, mmap: Mmap::default() }
    }

    /// Creates a brand new, empty segment file at `path`.
    pub(super) async fn create(path: PathBuf) -> io::Result<Segment> {
        File::create(path).await.map(Segment::new)
    }

    /// Opens the existing segment file at `path` read-only, for one
    /// found already on disk left over from a previous run.
    pub(super) async fn open(path: PathBuf) -> io::Result<Segment> {
        File::open(path).await.map(Segment::new)
    }

    /// Appends `buf` to this segment's file. Not yet durable on its own.
    /// Only ever called by `Committer`, on the active segment.
    pub(super) async fn append(&self, buf: Bytes) -> io::Result<()> {
        self.file.append(buf).await
    }

    /// Syncs everything appended so far to durable storage.
    pub(super) async fn flush(&self) -> io::Result<()> {
        self.file.flush().await
    }

    /// Reads `length` bytes at `offset`: sliced zero-copy from the
    /// mapping if one's been established, or via a positioned read
    /// against `file` if it isn't (not yet sealed, or sealing failed).
    pub(super) async fn read_at(&self, offset: u64, length: u32) -> io::Result<Bytes> {
        // `mmap.read_at`, not `seal`: this might still be the active
        // segment (still being written to), and mapping that one is
        // exactly what `seal` must never do -- only a sealed segment's
        // `mmap` is safe to establish on demand.
        if let Some(bytes) = self.mmap.read_at(offset, length) {
            return Ok(bytes);
        }
        self.file.read_at(offset, length).await
    }

    /// Establishes this segment's whole-file mapping, for a segment
    /// that's about to become (or already is) pending: from this point
    /// it's never written to again (see the module doc), so mapping it
    /// is safe. Idempotent, and, because `mmap` is shared, this update
    /// is visible through every clone of this `Segment` value already
    /// out there, including `Location`s `index` held from before
    /// sealing happened -- nothing needs to go back and update `index`
    /// itself.
    pub(super) fn seal(&self) -> io::Result<Bytes> {
        self.mmap.get_or_map(&self.file)
    }

    /// Whether `seal` has already established this segment's mapping.
    /// Exists for tests: proves the mapping is really shared through
    /// every clone of a `Segment` value, not just visible to whichever
    /// clone happened to call `seal`.
    #[cfg(test)]
    pub(super) fn mmap_established(&self) -> bool {
        self.mmap.get().is_some()
    }
}

impl File {
    /// Opens an existing segment file read-only, for one `Staging::open`
    /// found already on disk left over from a previous run.
    async fn open(path: PathBuf) -> io::Result<Self> {
        task::spawn_blocking(move || std::fs::File::open(path).map(Arc::new).map(File))
            .await
            .expect("open should not panic")
    }

    /// Creates a brand new segment file: readable and appendable, and
    /// failing if `path` already exists (a segment id is only ever used
    /// once).
    async fn create(path: PathBuf) -> io::Result<Self> {
        task::spawn_blocking(move || {
            let mut opts = std::fs::OpenOptions::new();
            opts.read(true).append(true).create_new(true);
            opts.open(path).map(Arc::new).map(File)
        })
        .await
        .expect("open should not panic")
    }

    /// Appends `buf`. Not yet durable on its own.
    async fn append(&self, buf: Bytes) -> io::Result<()> {
        let file = Arc::clone(&self.0);
        task::spawn_blocking(move || (&*file).write_all(&buf))
            .await
            .expect("write should not panic")
    }

    /// Syncs everything appended so far to durable storage.
    async fn flush(&self) -> io::Result<()> {
        let file = Arc::clone(&self.0);
        task::spawn_blocking(move || file.sync_data()).await.expect("flush should not panic")
    }

    /// Reads exactly `length` bytes at `offset`. Safe to call
    /// concurrently, including while another clone's `append` is
    /// running against the same underlying file.
    async fn read_at(&self, offset: u64, length: u32) -> io::Result<Bytes> {
        let file = Arc::clone(&self.0);
        let mut buf = vec![0u8; length as usize];
        task::spawn_blocking(move || -> io::Result<Vec<u8>> {
            read_at(&file, &mut buf, offset)?;
            Ok(buf)
        })
        .await
        .expect("read_at should not panic")
        .map(Bytes::from)
    }
}

impl Mmap {
    /// The mapping, if it's been established already. Never maps the
    /// file itself: callers that only want to peek (`read_at`, happy to
    /// fall back to `File::read_at` on a miss) shouldn't pay for
    /// mapping a whole segment just to read one small value from it.
    fn get(&self) -> Option<Bytes> {
        self.0.get().cloned()
    }

    /// Reads `length` bytes at `offset`, sliced zero-copy from the
    /// mapping -- if one's been established. `None`, not an error, if
    /// it hasn't: unlike `File::read_at`, there's nothing to actually
    /// read here, just an in-memory slice of what's already there.
    fn read_at(&self, offset: u64, length: u32) -> Option<Bytes> {
        let bytes = self.get()?;
        let start = offset as usize;
        Some(bytes.slice(start..start + length as usize))
    }

    /// The mapping, establishing it first if it isn't there yet.
    /// Idempotent: a mapping already established by another caller (or
    /// concurrently by this one losing a race) is reused as-is.
    fn get_or_map(&self, file: &File) -> io::Result<Bytes> {
        if let Some(bytes) = self.get() {
            return Ok(bytes);
        }
        // Safety: only ever called on a segment that's already fully
        // and durably written and sealed into `pending`.
        //
        // Not `MmapOptions::populate`: it would fault in the whole file
        // synchronously, inside a call this module doesn't spawn_blocking
        // on the assumption that `mmap(2)` itself is cheap. `open`'s
        // replay is the one call site where that might not hold.
        //
        // TODO: A losing caller still maps the file itself.
        // Switch to `get_or_try_init` once it stabilizes, to skip that.
        let mmap = unsafe { memmap2::Mmap::map(&*file.0)? };
        Ok(self.0.get_or_init(move || Bytes::from_owner(mmap)).clone())
    }
}

#[cfg(unix)]
fn pread(file: &std::fs::File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    use std::os::unix::fs::FileExt;
    file.read_at(buf, offset)
}
#[cfg(windows)]
fn pread(file: &std::fs::File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    use std::os::windows::fs::FileExt;
    file.seek_read(buf, offset)
}

/// Reads exactly `buf.len()` bytes from `file` starting at `offset`,
/// retrying on a short read. Safe to call concurrently from multiple
/// tasks, including while the committer is appending to the same `file`
/// through its own cursor.
fn read_at(file: &std::fs::File, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
    while !buf.is_empty() {
        let n = pread(file, buf, offset)?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "failed to read whole record",
            ));
        }
        buf = &mut buf[n..];
        offset += n as u64;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn seal_is_visible_through_every_earlier_clone() {
        let dir = testing::tempdir();
        let segment = Segment::create(dir.path().join("0.log")).await.unwrap();

        // Cloned before `seal` runs: no mapping yet, since `Mmap` starts
        // empty and `create` never establishes one.
        let clone = segment.clone();
        assert!(!clone.mmap_established());

        segment.seal().unwrap();

        // The clone shares `segment`'s `mmap` cell, so it sees the
        // mapping `seal` just established on the other clone -- proving
        // this is a shared cell, not a fresh mapping each clone would
        // have to establish for itself.
        assert!(clone.mmap_established());
    }
}
