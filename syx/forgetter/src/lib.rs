//! An append-only log written to a private directory.
//!
//! Content is keyed by its own digest, so replaying the same (key, value) pair
//! after a crash, or retrying the same segment after a failed flush, is always
//! a safe no-op.
//!
//! Every log file starts with a fixed magic prefix, before any
//! records. Past that prefix, records are framed as:
//! `[key: 32 bytes][value_len: u32 BE][value]`.

use std::collections::HashMap;
use std::io;
use std::path::{
    Path,
    PathBuf,
};
use std::sync::Arc;

use bytes::Bytes;
use content_addressing::{
    Codec,
    Digest,
    Hasher,
};
use crossbeam_skiplist::SkipMap;
use tokio::{
    fs,
    task,
};

mod committer;
mod file_id;
mod segment;

use committer::RECORD_HEADER_LEN;
pub use file_id::FileId;
use segment::{
    BytesIndex,
    Segment,
};

#[cfg(test)]
mod tests;

/// Where a record's value lives within a segment's bytes: `offset`/
/// `length` point past the `[key][len]` header, at the value.
#[derive(Clone, Copy)]
pub struct Slot {
    /// Byte offset of the value, past the `[key][len]` header.
    pub offset: u64,
    /// Length of the value in bytes.
    pub length: u32,
}

impl BytesIndex for Slot {
    async fn read_from(&self, segment: &Segment) -> io::Result<Bytes> {
        (self.offset..self.offset + self.length as u64).read_from(segment).await
    }
}

/// A segment's records, keyed by digest for the O(1) lookup `get`/
/// `contains` need. Order doesn't matter: each `Slot` is a self-
/// contained byte range, independent of every other one.
pub type Records = HashMap<Digest, Slot>;

/// One entry of `pending`: a rotated-out segment together with every
/// record it holds. Same shape as the committer's own `Active`, but
/// immutable.
#[derive(Clone)]
struct Pending {
    segment: Segment,
    records: Records,
}

/// A place to durably drop content and forget about it, until it's ready
/// to be packed elsewhere. `committer::spawn` runs the actual write path
/// in its own task; `Forgetter` holds the same active/pending state
/// through shared `Arc`s, so its own reads never see something the
/// committer doesn't.
pub struct Forgetter {
    /// The directory segments live in.
    dir:         PathBuf,
    /// Observes and drives the running committer task; see `Handle`'s
    /// own doc.
    handle:      committer::Handle,
    /// Rotated out, not yet forgotten; shared with the committer,
    /// which is the only thing that ever inserts into it.
    pending:     Arc<SkipMap<FileId, Pending>>,
    /// `put` refuses new writes once pending segments reach this many,
    /// so a persistently failing `flush_pending` bounds local disk usage
    /// instead of growing it without limit.
    max_pending: u16,
}

impl Forgetter {
    /// Opens `dir`, creating it if needed, and replays whatever segments
    /// are already there.
    ///
    /// `codec` decodes each replayed record to verify it against its
    /// own key; it must match whatever the caller itself uses, since a
    /// value's digest is only meaningful once decoded back to its
    /// original content.
    ///
    /// `max_pending` is enforced from this point on, even if replay
    /// already found more pending segments than that: `put` refuses
    /// further writes until enough of the backlog clears.
    pub async fn open(dir: impl Into<PathBuf>, codec: Codec, max_pending: u16) -> io::Result<Self> {
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
        for (i, &id) in ids.iter().enumerate() {
            let path = file_id::path(&dir, id);
            // Every earlier file was already durable before this
            // process ever rotated past it; only the one that could
            // have been active when the previous run stopped needs
            // verifying.
            let verify = (i + 1 == ids.len()).then_some(codec);
            if let Some((segment, records)) = revalidate_segment(&path, verify).await? {
                pending.insert(id, Pending { segment, records });
            }
        }

        let handle = committer::spawn(dir.clone(), codec, Arc::clone(&pending)).await?;

        Ok(Self { dir, handle, pending, max_pending })
    }

    /// The `Pending` entry for `id`. Panics if it isn't tracked: its
    /// only caller, `segment_bytes`, only ever asks for one it already
    /// knows must still be open, from `pending_segments`/`rotate`'s own
    /// return value, so this is never asked about the active segment.
    fn find(&self, id: FileId) -> Pending {
        self.pending
            .get(&id)
            .map(|entry| entry.value().clone())
            .unwrap_or_else(|| panic!("segment {id} is no longer tracked"))
    }

    /// Durably appends `value` under `key` to the active segment. Once
    /// this returns, `value` survives a crash of this node. Returns the
    /// active segment's length immediately after.
    ///
    /// Refuses the write if pending segments already number `max_pending`:
    /// at that point `flush_pending` isn't keeping up, and accepting more
    /// would grow local disk usage without bound.
    pub async fn put(&self, key: Digest, value: Bytes) -> io::Result<u64> {
        let pending_len = self.pending.len();
        if pending_len >= self.max_pending as usize {
            return Err(io::Error::other(format!(
                "forgetter: {pending_len} segments already pending (max {})",
                self.max_pending
            )));
        }
        self.handle.put(key, value).await
    }

    /// Fetches the value staged under `key`, if any. Checks the active
    /// segment first, then every pending one.
    pub async fn get(&self, key: Digest) -> io::Result<Option<Bytes>> {
        if let Some(bytes) = self.handle.get(key).await? {
            return Ok(Some(bytes));
        }
        for entry in self.pending.iter() {
            if let Some(&slot) = entry.value().records.get(&key) {
                return entry.value().segment.bytes(slot).await.map(Some);
            }
        }
        Ok(None)
    }

    /// Whether `key` is currently staged, without reading its value.
    pub async fn contains(&self, key: Digest) -> bool {
        if self.handle.contains(key).await {
            return true;
        }
        self.pending.iter().any(|entry| entry.value().records.contains_key(&key))
    }

    /// How many bytes the active segment currently holds. `put` already
    /// returns this for its own caller; this is for querying it
    /// independently of any particular write, e.g. a caller checking
    /// whether the active segment has anything worth rotating.
    pub fn active_segment_len(&self) -> u64 {
        self.handle.active_segment_len()
    }

    /// Closes the active segment out as a new pending segment and starts
    /// a fresh one. Returns the segment that was just closed out, for
    /// `entries`/`forget`.
    pub async fn rotate(&self) -> io::Result<FileId> {
        self.handle.rotate().await
    }

    /// Segments rotated out but not yet forgotten. Covers both a
    /// previous flush that failed partway and, after a restart, whatever
    /// `open` found already on disk.
    pub fn pending_segments(&self) -> Vec<FileId> {
        self.pending.iter().map(|entry| *entry.key()).collect()
    }

    /// The sealed segment's records, and every one's key, offset, and
    /// length within them. `id` must have come from `rotate` or
    /// `pending_segments`; it is never the active segment.
    pub async fn segment_bytes(&self, id: FileId) -> io::Result<(Bytes, Records)> {
        let pending = self.find(id);
        let buf = pending.segment.bytes(..).await?;
        Ok((buf, pending.records))
    }

    /// Deletes `id`'s segment. Only call this once its content is
    /// durably packed elsewhere. `id` must be pending, never the active
    /// segment: `pending` structurally can't hold that one, so there's
    /// nothing to guard against here.
    ///
    /// Just removes `id` from `pending`: there's no second, flat index
    /// to keep in sync with it, so nothing else needs touching before
    /// the file itself comes off disk.
    pub async fn forget(&self, id: FileId) -> io::Result<()> {
        self.pending.remove(&id);
        fs::remove_file(self.file_path(id)).await
    }

    fn file_path(&self, id: FileId) -> PathBuf {
        file_id::path(&self.dir, id)
    }

    /// Every `(key, value)` pair in `id`'s segment. Each value is a
    /// zero-copy `Bytes` view into `segment_bytes`'s buffer.
    ///
    /// Test-only: a real caller doing `flush_segments`-style packing
    /// uses `segment_bytes` directly instead, since it needs the record
    /// offsets/lengths, not decoded values.
    #[cfg(test)]
    async fn entries(&self, id: FileId) -> io::Result<Vec<(Digest, Bytes)>> {
        let (buf, records) = self.segment_bytes(id).await?;
        Ok(records
            .into_iter()
            .map(|(key, slot)| {
                (
                    key,
                    buf.slice(
                        slot.offset as usize..(slot.offset + u64::from(slot.length)) as usize,
                    ),
                )
            })
            .collect())
    }
}

/// Parses records from `buf` in order, from the start, and returning
/// each one's key and where its value lives, plus how many bytes from
/// the start of `buf` are valid. `buf` is expected to already be a
/// segment's records on their own.
///
/// `codec` is `None` for a segment already known to be fully durable, in
/// which case only the length framing is trusted. It's `Some` whenever
/// the segment's tail might be torn: each record is then decoded and
/// its digest recomputed against its own key, and the first record that
/// fails this, or that the file simply doesn't have enough bytes left for,
/// is treated as a torn tail. Everything from that point on is dropped.
fn parse(buf: &Bytes, codec: Option<Codec>) -> (u64, Records) {
    let mut records = Records::new();
    let mut offset = 0usize;
    while offset as u64 + RECORD_HEADER_LEN <= buf.len() as u64 {
        let key = Digest::new(buf[offset..offset + 32].try_into().unwrap());
        let length = u32::from_be_bytes(buf[offset + 32..offset + 36].try_into().unwrap());
        let value_start = offset + 36;
        let value_end = value_start + length as usize;
        if value_end > buf.len() {
            break;
        }
        if let Some(codec) = codec {
            let value = buf.slice(value_start..value_end);
            match codec.decode(value) {
                Ok((_, decoded)) if Hasher::new().part(&decoded).digest() == key => {}
                _ => break,
            }
        }
        records.insert(key, Slot { offset: value_start as u64, length });
        offset = value_end;
    }
    (offset as u64, records)
}

/// Re-reads the segment file at `path`, verifying and truncating away any
/// torn tail.
///
/// The returned `Segment` is sealed: both `Segment::open` and `truncate`
/// seal before handing one back, so callers don't need to do it again.
async fn revalidate_segment(
    path: &Path,
    verify: Option<Codec>,
) -> io::Result<Option<(Segment, Records)>> {
    let Some(segment) = Segment::open(path).await? else {
        // Not confidently one of `forgetter`'s own; see `Segment::open`.
        // Left untouched, whatever it is.
        return Ok(None);
    };

    let buf = segment.bytes(..).await?;
    let len = buf.len() as u64;
    let (valid_len, records) = {
        let buf = buf.clone();
        task::spawn_blocking(move || parse(&buf, verify)).await.expect("parse should not panic")
    };
    let segment = if valid_len < len {
        Segment::truncate(path.to_owned(), valid_len).await?
    } else {
        segment
    };
    if records.is_empty() {
        fs::remove_file(path).await?;
        return Ok(None);
    }

    Ok(Some((segment, records)))
}
