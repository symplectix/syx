//! An append-only log written to a private directory.
//!
//! A value is durable once `save` returns, so replaying the same
//! append after a crash, or retrying a failed flush, is always safe:
//! this crate never interprets what it stores, so it's on the caller to
//! make retries idempotent (e.g. by addressing values by their own
//! content, so appending the same bytes twice is a safe no-op).
//!
//! Every log file starts with a fixed magic prefix, before any
//! records. Past that prefix, records are framed as:
//! `[value_len: u32 BE][value]`.

use std::io;
use std::path::{
    Path,
    PathBuf,
};
use std::sync::Arc;

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use tokio::fs;

mod committer;
mod file_id;
mod segment;

use committer::RECORD_HEADER_LEN;
pub use file_id::FileId;
use segment::BytesIndex;
pub use segment::Segment;

#[cfg(test)]
mod tests;

/// Where a record's value lives within a segment's bytes: `offset`/
/// `length` point past the `[len]` header, at the value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Slot {
    /// Byte offset of the value, past the `[len]` header.
    pub offset: u64,
    /// Length of the value in bytes.
    pub length: u32,
}

impl BytesIndex for Slot {
    async fn read_from(&self, segment: &Segment) -> io::Result<Bytes> {
        (self.offset..self.offset + self.length as u64).read_from(segment).await
    }
}

/// Where a record physically lives: which segment, and where within it.
/// Carries the `Segment` itself (a cheap, `Clone`-able handle) internally,
/// not just `file`, so `bytes` can read directly from it without a
/// separate lookup back into `Forgetter` that could race against that
/// same segment being forgotten in between. `file`/`segment`/`slot`
/// stay private: a `Locator` only ever comes from `save` or `open`'s
/// replay, never built by hand from mismatched parts.
#[derive(Clone)]
pub struct Locator {
    file:    FileId,
    segment: Segment,
    slot:    Slot,
}

impl Locator {
    /// Which segment the record is in.
    pub fn file(&self) -> FileId {
        self.file
    }

    /// Where within that segment.
    pub fn slot(&self) -> Slot {
        self.slot
    }

    /// Reads the bytes this locator points at, straight from its own
    /// segment: no separate lookup back into `Forgetter` that could
    /// race against that same segment being forgotten in between.
    pub async fn bytes(&self) -> io::Result<Bytes> {
        self.segment.bytes(self.slot).await
    }
}

impl std::fmt::Debug for Locator {
    // `segment` has no meaningful printable state of its own; `file`/
    // `slot` already identify a `Locator` uniquely.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Locator").field("file", &self.file).field("slot", &self.slot).finish()
    }
}

/// The segment `find` located a `FileId` in: still the one being
/// appended to, or already sealed into `pending`. Reading works the
/// same either way; the distinction just tells a caller whether the
/// `FileId` alone could still change what `active_segment_len`/`rotate`
/// report next, or is settled for good.
pub enum Found {
    /// Still being appended to by the committer.
    Active(Segment),
    /// Sealed; rotated out into `pending`.
    Pending(Segment),
}

impl Found {
    /// The segment either way, regardless of which variant this is.
    pub fn segment(&self) -> &Segment {
        match self {
            Found::Active(segment) | Found::Pending(segment) => segment,
        }
    }
}

/// A place to durably drop content and forget about it, until it's ready
/// to be packed elsewhere. `committer::spawn` runs the actual write path
/// in its own task; `Forgetter` holds the same active/pending state
/// through shared `Arc`s, so its own reads never see something the
/// committer doesn't.
///
/// Doesn't know or care what's inside a value: a caller that wants
/// values addressable by their own key encodes that key into the bytes
/// it hands to `save` itself, and decodes it back out of whatever
/// `find`/`Found::segment` later reads.
pub struct Forgetter {
    /// The directory segments live in.
    dir:         PathBuf,
    /// Observes and drives the running committer task; see `Handle`'s
    /// own doc.
    handle:      committer::Handle,
    /// Rotated out, not yet forgotten; shared with the committer,
    /// which is the only thing that ever inserts into it.
    pending:     Arc<SkipMap<FileId, Segment>>,
    /// `save` refuses new writes once pending segments reach this many,
    /// so a persistently failing caller-side flush bounds local disk
    /// usage instead of growing it without limit.
    max_pending: u16,
}

impl Forgetter {
    /// Opens `dir`, creating it if needed, and replays whatever segments
    /// are already there.
    ///
    /// `max_pending` is enforced from this point on, even if replay
    /// already found more pending segments than that: `save` refuses
    /// further writes until enough of the backlog clears.
    ///
    /// The second return value is every record recovered from segments
    /// already on disk, each alongside the exact bytes it was `save`d
    /// with, for the caller to verify and index however it needs to.
    /// This crate never verifies content itself; see the module doc.
    pub async fn open(
        dir: impl Into<PathBuf>,
        max_pending: u16,
    ) -> io::Result<(Self, Vec<(Locator, Bytes)>)> {
        let dir = dir.into();
        fs::create_dir_all(&dir).await?;

        let mut ids = Vec::new();
        let mut read_dir = fs::read_dir(&dir).await?;
        while let Some(entry) = read_dir.next_entry().await? {
            if let Some(id) = file_id::parse(&entry.file_name()) {
                ids.push(id);
            }
        }
        ids.sort_unstable();

        // Seeded from the highest id found, `ids` already being sorted,
        // before anything is created fresh, so a freshly claimed id can
        // never collide with one left over from a previous run.
        if let Some(&last) = ids.last() {
            file_id::seed(last);
        }

        let pending = Arc::new(SkipMap::new());
        let mut replayed = Vec::new();
        for &id in ids.iter() {
            let path = file_id::path(&dir, id);
            if let Some((segment, slots)) = revalidate_segment(&path).await? {
                let buf = segment.bytes(..).await?;
                replayed.extend(slots.into_iter().map(|slot| {
                    let bytes = slot.read(&buf);
                    (Locator { file: id, segment: segment.clone(), slot }, bytes)
                }));
                pending.insert(id, segment);
            }
        }

        let handle = committer::spawn(dir.clone(), Arc::clone(&pending)).await?;

        Ok((Self { dir, handle, pending, max_pending }, replayed))
    }

    /// Durably appends `value` to the active segment. Once this
    /// returns, `value` survives a crash of this node.
    ///
    /// Refuses the write if pending segments already number
    /// `max_pending`: at that point the caller isn't keeping up with
    /// clearing them, and accepting more would grow local disk usage
    /// without bound.
    pub async fn save(&self, value: Bytes) -> io::Result<Locator> {
        let pending_len = self.pending.len();
        if pending_len >= self.max_pending as usize {
            return Err(io::Error::other(format!(
                "forgetter: {pending_len} segments already pending (max {})",
                self.max_pending
            )));
        }
        self.handle.save(value).await
    }

    /// Returns the segment currently lives in, if it's tracked at all
    /// (as the active segment, or still pending). `None` once `id` has
    /// been forgotten.
    pub async fn find(&self, id: FileId) -> Option<Found> {
        if let Some(segment) = self.handle.segment_if_active(id).await {
            return Some(Found::Active(segment));
        }
        self.pending.get(&id).map(|entry| Found::Pending(entry.value().clone()))
    }

    /// How many bytes the active segment currently holds. `save`
    /// already returns this for its own caller; this is for querying it
    /// independently of any particular write, e.g. a caller checking
    /// whether the active segment has anything worth rotating.
    pub fn active_segment_len(&self) -> u64 {
        self.handle.active_segment_len()
    }

    /// Closes the active segment out as a new pending segment and starts
    /// a fresh one. Returns the segment that was just closed out.
    pub async fn rotate(&self) -> io::Result<FileId> {
        self.handle.rotate().await
    }

    /// Segments rotated out but not yet forgotten. Covers both a
    /// previous flush that failed partway and, after a restart, whatever
    /// `open` found already on disk.
    pub fn pending_segments(&self) -> Vec<FileId> {
        self.pending.iter().map(|entry| *entry.key()).collect()
    }

    /// Deletes `id`'s segment. Only call this once its content is
    /// durably packed elsewhere. `id` must be pending, never the active
    /// segment: `pending` structurally can't hold that one, so there's
    /// nothing to guard against here.
    ///
    /// Just removes `id` from `pending`: there's no second, flat index
    /// to keep in sync with it, so nothing else needs touching before
    /// the file itself comes off disk.
    ///
    /// A caller holding a `Locator` into this segment from before this
    /// call keeps working after it: `Locator::segment` is already an
    /// open handle, and on POSIX, deleting a file doesn't invalidate
    /// handles opened before the delete. This assumes POSIX unlink
    /// semantics; on Windows, `remove_file` here would fail outright
    /// while such a handle is still open, since `Segment`'s file isn't
    /// opened with `FILE_SHARE_DELETE`. Not fixed, since this crate
    /// currently only runs on Linux.
    pub async fn forget(&self, id: FileId) -> io::Result<()> {
        self.pending.remove(&id);
        fs::remove_file(self.file_path(id)).await
    }

    fn file_path(&self, id: FileId) -> PathBuf {
        file_id::path(&self.dir, id)
    }
}

impl Slot {
    fn read(&self, buf: &Bytes) -> Bytes {
        buf.slice(self.offset as usize..(self.offset + u64::from(self.length)) as usize)
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
fn parse(buf: &Bytes) -> (u64, Vec<Slot>) {
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
    (offset as u64, slots)
}

/// Re-reads the segment file at `path`, discarding any torn tail found
/// past the last structurally complete record.
///
/// The returned `Segment` is sealed: both `Segment::open` and `truncate`
/// seal before handing one back, so callers don't need to do it again.
async fn revalidate_segment(path: &Path) -> io::Result<Option<(Segment, Vec<Slot>)>> {
    let Some(segment) = Segment::open(path).await? else {
        // Not confidently one of `forgetter`'s own; see `Segment::open`.
        // Left untouched, whatever it is.
        return Ok(None);
    };

    let buf = segment.bytes(..).await?;
    let len = buf.len() as u64;
    let (valid_len, slots) = parse(&buf);
    let segment = if valid_len < len {
        Segment::truncate(path.to_owned(), valid_len).await?
    } else {
        segment
    };
    if slots.is_empty() {
        fs::remove_file(path).await?;
        return Ok(None);
    }

    Ok(Some((segment, slots)))
}
