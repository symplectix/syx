//! A segment's on-disk file and in-memory view.
//! A segment is created empty, then appended to sequentially while
//! it's the active one; once rotated out it never changes again.

use std::io;
use std::io::Write as _;
use std::ops::{
    Bound,
    RangeBounds,
};
use std::path::{
    Path,
    PathBuf,
};
use std::sync::{
    Arc,
    OnceLock,
};

use bytes::Bytes;
use tokio::{
    fs,
    task,
};

use super::Slot;
use super::committer::RECORD_HEADER_LEN;

/// Every segment file starts with exactly these bytes.
/// `open_raw` uses this to tell a real segment apart from some other
/// file that happens to match `FileId`'s naming.
const MAGIC: &[u8] = b"SEGv1";

/// Where a segment's own records start, past `MAGIC`.
const MAGIC_LEN: u64 = MAGIC.len() as u64;

/// A segment's open file, whether it's the one currently being appended
/// to or one already rotated out. Reading works the same either way;
/// only `forgetter`'s own committer ever writes to one.
#[derive(Clone)]
pub struct Segment {
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
/// independent operations only: `read_at`/`read_from` for concurrent
/// reads, `append` for the one writer, and `mmap` for a one-time
/// whole-file mapping.
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

    /// Creates a brand new segment file at `path`, and writes `MAGIC` as
    /// its first bytes.
    pub(super) async fn create(path: PathBuf) -> io::Result<Segment> {
        let segment = File::create(path).await.map(Segment::new)?;
        segment.append(Bytes::from_static(MAGIC)).await?;
        Ok(segment)
    }

    /// Opens the segment file at `path`, checking that it's really one
    /// of `forgetter`'s own before treating it as one.
    ///
    /// Returns `None` for anything that isn't confidently one of
    /// `forgetter`'s own segments.
    ///
    /// Seals a `Some` result before returning it: once `MAGIC` is
    /// confirmed, this is a real segment, and this call's own view of
    /// it never changes again.
    async fn open_raw(path: &Path) -> io::Result<Option<Segment>> {
        let segment = File::open(path.to_owned()).await.map(Segment::new)?;
        match segment.file.read_at(0, MAGIC_LEN as u32).await {
            Ok(prefix) if &prefix[..] == MAGIC => {
                let _ = segment.seal();
                Ok(Some(segment))
            }
            Ok(_) => Ok(None),
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Recovers the segment file at `path`: confirms it's really one of
    /// `forgetter`'s own (`open_raw`), then reads its records
    /// structurally and discards any torn tail found past the last
    /// complete one. Deletes `path` entirely if nothing valid remains.
    ///
    /// Returns `None` for anything that isn't confidently one of
    /// `forgetter`'s own segments, or that turned out to hold no valid
    /// records at all.
    pub(super) async fn open(path: &Path) -> io::Result<Option<(Segment, Slots)>> {
        let Some(segment) = Self::open_raw(path).await? else {
            // Not confidently one of `forgetter`'s own. Left untouched,
            // whatever it is.
            return Ok(None);
        };

        let buf = segment.bytes(..).await?;
        let len = buf.len() as u64;
        let (valid_len, slots) = parse(&buf);
        let segment = if valid_len < len {
            Self::truncate(path.to_owned(), valid_len).await?
        } else {
            segment
        };
        if slots.is_empty() {
            fs::remove_file(path).await?;
            return Ok(None);
        }

        Ok(Some((segment, slots)))
    }

    /// Truncates the segment file at `path` to `len` bytes of records,
    /// discarding a torn tail found while revalidating it after a
    /// crash, then reopens it through `open_raw`.
    ///
    /// `len` is record-relative: this adds `MAGIC_LEN` back before
    /// touching the file, so the caller never needs to know it exists.
    ///
    /// `MAGIC` itself always survives, whatever `len` is, since the
    /// file is never truncated to fewer than `MAGIC_LEN` bytes. So this
    /// always finds it, unless opening the truncated file failed in
    /// some other way, which is surfaced as an error rather than
    /// swallowed quietly.
    async fn truncate(path: PathBuf, len: u64) -> io::Result<Segment> {
        let len = MAGIC_LEN + len;
        fs::OpenOptions::new().write(true).open(&path).await?.set_len(len).await?;
        Self::open_raw(&path).await?.ok_or_else(|| {
            io::Error::other(format!(
                "forgetter: {path:?} lost its magic after truncating to {len} bytes"
            ))
        })
    }

    /// Appends `buf` to this segment's file. Not yet durable on its own.
    /// Only ever called by `Committer`, on the active segment.
    pub(super) async fn append(&self, buf: Bytes) -> io::Result<()> {
        assert!(!self.sealed());
        self.file.append(buf).await
    }

    /// Syncs everything appended so far to durable storage.
    pub(super) async fn flush(&self) -> io::Result<()> {
        assert!(!self.sealed());
        self.file.flush().await
    }

    /// This segment's records at `index`, sliced zero-copy from the
    /// mapping `seal` established, if there is one. `index` is
    /// record-relative: 0 means the first byte past `MAGIC`, not the
    /// first byte of the file.
    pub async fn bytes(&self, index: impl BytesIndex) -> io::Result<Bytes> {
        index.read_from(self).await
    }

    /// Every record position in this sealed segment, parsed directly
    /// from its own bytes the same way replay does, independent of
    /// whatever index a caller keeps on the side. Errors for a
    /// still-active segment.
    pub async fn slots(&self) -> io::Result<impl Iterator<Item = Slot> + Send> {
        if !self.sealed() {
            return Err(io::Error::other("forgetter: slots() called on a still-active segment"));
        }
        let buf = self.bytes(..).await?;
        Ok(parse(&buf).1)
    }

    /// Establishes this segment's whole-file mapping, for a segment
    /// that's about to become pending or already is.
    ///
    /// From this point on the segment is never written to again.
    pub(super) fn seal(&self) -> io::Result<Bytes> {
        self.mmap.get_or_map(&self.file)
    }

    /// Whether `seal` has already established this segment's mapping.
    /// `append`/`flush` assert this is false: once sealed, a segment is
    /// never written to again. Also used by tests, to prove the mapping
    /// is really shared through every clone of a `Segment` value, not
    /// just visible to whichever clone happened to call `seal`.
    pub(super) fn sealed(&self) -> bool {
        self.mmap.get().is_some()
    }
}

/// Every record position `parse` found in one segment's bytes, in order.
pub(super) struct Slots(std::vec::IntoIter<Slot>);

impl Slots {
    fn is_empty(&self) -> bool {
        self.0.len() == 0
    }
}

impl Iterator for Slots {
    type Item = Slot;

    fn next(&mut self) -> Option<Slot> {
        self.0.next()
    }
}

/// Parses records from `buf` in order, from the start, purely
/// structurally: `[len: u32][value]` frames, with no interpretation of
/// `value` at all. Returns every record's position, plus how many bytes
/// from the start of `buf` are valid.
///
/// Stops at the first record whose declared length runs past the end of
/// `buf`; everything from that point on is dropped, since a length that
/// long can only mean the write that produced it never finished. This
/// crate doesn't verify a record's content beyond that: a caller that
/// wants to also detect bit-level corruption within an otherwise
/// well-framed record does so itself, using `find`'s replay output.
fn parse(buf: &Bytes) -> (u64, Slots) {
    let mut slots = Vec::new();
    let mut offset = 0usize;
    while offset as u64 + RECORD_HEADER_LEN <= buf.len() as u64 {
        let length = u32::from_be_bytes(buf[offset..offset + 4].try_into().unwrap());
        let value_start = offset + 4;
        let value_end = value_start + length as usize;
        if value_end > buf.len() {
            break;
        }
        slots.push(Slot { offset: value_start as u64, length });
        offset = value_end;
    }
    (offset as u64, Slots(slots.into_iter()))
}

/// Something that can select a byte range within a `Segment`'s records.
///
/// Every index is record-relative; `read_from`'s own implementations are
/// where `MAGIC_LEN` gets added back before actually touching the
/// file, once and only here.
pub trait BytesIndex {
    /// Reads the bytes this index selects from `segment`.
    async fn read_from(&self, segment: &Segment) -> io::Result<Bytes>;
}

impl<T: RangeBounds<u64>> BytesIndex for T {
    async fn read_from(&self, segment: &Segment) -> io::Result<Bytes> {
        let start = MAGIC_LEN
            + match self.start_bound() {
                Bound::Included(&start) => start,
                Bound::Excluded(&start) => start + 1,
                Bound::Unbounded => 0,
            };
        let end = match self.end_bound() {
            Bound::Included(&end) => Some(MAGIC_LEN + end + 1),
            Bound::Excluded(&end) => Some(MAGIC_LEN + end),
            Bound::Unbounded => None,
        };

        if let Some(bytes) = segment.mmap.get() {
            let end = end.unwrap_or(bytes.len() as u64);
            return Ok(bytes.slice(start as usize..end as usize));
        }
        match end {
            Some(end) => segment.file.read_at(start, (end - start) as u32).await,
            None => segment.file.read_from(start).await,
        }
    }
}

impl File {
    /// Opens an existing segment file read-only, for one `Forgetter::open`
    /// found already on disk left over from a previous run.
    async fn open(path: PathBuf) -> io::Result<Self> {
        task::spawn_blocking(move || std::fs::File::open(path).map(Arc::new).map(File))
            .await
            .expect("open should not panic")
    }

    /// Creates a brand new segment file: readable and appendable, and
    /// failing if `path` already exists, since a segment id is only ever
    /// used once.
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

    /// Reads from `start` to this file's current end, in one shot,
    /// discovering how far that is along the way.
    async fn read_from(&self, start: u64) -> io::Result<Bytes> {
        let file = Arc::clone(&self.0);
        task::spawn_blocking(move || -> io::Result<Vec<u8>> {
            let len = file.metadata()?.len();
            let mut buf = vec![0u8; (len - start) as usize];
            read_at(&file, &mut buf, start)?;
            Ok(buf)
        })
        .await
        .expect("read_from should not panic")
        .map(Bytes::from)
    }
}

impl Mmap {
    /// The mapping, if it's been established already. Never maps the
    /// file itself.
    fn get(&self) -> Option<Bytes> {
        self.0.get().cloned()
    }

    /// The mapping, establishing it first if it isn't there yet.
    /// Idempotent: a mapping already established by another caller, or
    /// concurrently by this one losing a race, is reused as-is.
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
        assert!(!clone.sealed());

        segment.seal().unwrap();

        // The clone shares `segment`'s `mmap` cell, so it sees the
        // mapping `seal` just established on the other clone. This
        // proves it's a shared cell, not a fresh mapping each clone
        // would have to establish for itself.
        assert!(clone.sealed());
    }

    #[tokio::test]
    async fn slots_errors_on_a_still_active_segment() {
        let dir = testing::tempdir();
        let segment = Segment::create(dir.path().join("0.log")).await.unwrap();

        assert!(!segment.sealed());
        assert!(segment.slots().await.is_err());
    }
}
