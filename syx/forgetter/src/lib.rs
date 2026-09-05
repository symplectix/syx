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

use std::collections::BTreeMap;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use tokio::fs;

mod committer;
mod file_id;
mod segment;

pub use file_id::FileId;
pub use segment::Segment;
use segment::{
    BytesIndex,
    Slots,
};

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

/// What `Forgetter::open` recovered from segments already on disk.
/// Assembling a `Locator` needs nothing beyond what's already in each
/// segment's own `(FileId, Segment, Slots)`, so this just chains all of
/// them into one `Locator` sequence rather than building its own state
/// machine to walk them one segment at a time.
pub struct Replay(Box<dyn Iterator<Item = Locator> + Send>);

impl Replay {
    fn new(segments: BTreeMap<FileId, (Segment, Slots)>) -> Self {
        Self(Box::new(segments.into_iter().flat_map(|(file, (segment, slots))| {
            slots.map(move |slot| Locator { file, slot, segment: segment.clone() })
        })))
    }
}

impl Iterator for Replay {
    type Item = Locator;

    fn next(&mut self) -> Option<Locator> {
        self.0.next()
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
    Sealed(Segment),
}

impl Found {
    /// The segment either way, regardless of which variant this is.
    pub fn segment(&self) -> &Segment {
        match self {
            Found::Active(segment) | Found::Sealed(segment) => segment,
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
    rotated:     tokio::sync::watch::Sender<()>,
    /// `save` refuses new writes once pending segments reach this many.
    /// Not primarily about bounding local disk usage: it's what
    /// guarantees a caller-side flush that's stuck failing can't stay
    /// invisible forever just because nobody happens to be watching for
    /// it. A caller that already checks its own flush health some other
    /// way still hits this eventually if that check is ignored.
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
    /// `rotate_threshold` rotates the active segment out on its own,
    /// the moment a write lands that takes it past this many bytes, the
    /// same way a typical logger's size-based rollover works: the
    /// caller only decides how big a segment gets to be, never when the
    /// rotation itself happens.
    ///
    /// `rotate_after`, if set, also rotates on this cadence regardless
    /// of size, so a segment that never crosses `rotate_threshold` still
    /// doesn't sit active forever, the way a typical logger's own
    /// time-based rollover works. `None` leaves rotation purely size-
    /// and caller-driven.
    ///
    /// The second return value is every record recovered from segments
    /// already on disk, for the caller to verify and index however it
    /// needs to; `Locator::bytes` reads a record's own content back
    /// (this crate never verifies content itself; see the module doc).
    pub async fn open(
        dir: impl Into<PathBuf>,
        max_pending: u16,
        rotate_threshold: u64,
        rotate_after: Option<Duration>,
    ) -> io::Result<(Self, Replay)> {
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
        let mut segments = BTreeMap::new();
        for &file in ids.iter() {
            let path = file_id::path(&dir, file);
            if let Some((segment, slots)) = Segment::open(&path).await? {
                pending.insert(file, segment.clone());
                segments.insert(file, (segment, slots));
            }
        }

        let (rotated, _) = tokio::sync::watch::channel(());
        let handle = committer::spawn(
            dir.clone(),
            Arc::clone(&pending),
            rotated.clone(),
            rotate_threshold,
            rotate_after,
        )
        .await?;

        Ok((Self { dir, handle, pending, rotated, max_pending }, Replay::new(segments)))
    }

    /// Durably appends `value` to the active segment. Once this
    /// returns, `value` survives a crash of this node.
    ///
    /// Refuses the write if pending segments already number
    /// `max_pending`: at that point whatever's supposed to be clearing
    /// them has clearly stopped working, and this is what forces that
    /// fact into the open instead of letting writes keep landing on top
    /// of a backlog nobody is draining.
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
        self.pending.get(&id).map(|entry| Found::Sealed(entry.value().clone()))
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

    /// Whether any segment is currently pending. Cheaper than checking
    /// `pending_segments().is_empty()`, for a caller that only needs to
    /// know whether there's anything to flush at all.
    pub fn has_pending(&self) -> bool {
        !self.pending.is_empty()
    }

    /// Subscribes to rotation events. `changed()` on the returned
    /// receiver resolves on every rotation, from any cause, until every
    /// sender (this `Forgetter`'s own, and the committer's) is dropped,
    /// after which it returns `Err` instead, the same way
    /// `mpsc::Receiver::recv` returns `None` once every `Sender` is
    /// gone. A caller can hold the receiver across an unbounded wait
    /// without that keeping this `Forgetter` alive.
    ///
    /// Reuse one receiver to react to every rotation: like any `watch`
    /// channel, it only remembers the latest change, not a history of
    /// every rotation that happened while nobody was watching.
    pub fn rotated(&self) -> tokio::sync::watch::Receiver<()> {
        self.rotated.subscribe()
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
