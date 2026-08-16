//! `Cas`'s local staging area: an append-only log written to a private
//! directory, not to `db` or `store`. Every `put` fsyncs before returning,
//! so what's staged here survives this node's own crash, unlike
//! `slatedb`'s in-memory WAL buffer. Content is keyed by its own digest,
//! so replaying the same (key, value) pair after a crash, or retrying the
//! same segment after a failed flush, is always a safe no-op.
//!
//! Records are framed as `[key: 32 bytes][value_len: u32 BE][value]`, with
//! no separate checksum. `Bitcask::open` verifies the records it replays
//! by decoding them and recomputing their digest, which is a stronger
//! check than a CRC would be, and only needs to run against the one file
//! that could have been mid-write when a crash happened; every earlier
//! file was already durable before this process ever rotated past it.
//!
//! Puts append to one active segment. Once a caller rotates it out, it
//! becomes a pending segment: readable through `entries`, and removed via
//! `finish` once its content is durably packed elsewhere. `open` always
//! starts a fresh active segment; every file found on disk becomes a
//! pending segment, so recovering from a crash needs no special case
//! beyond what a failed flush already handles.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::{
    Path,
    PathBuf,
};
use std::sync::atomic::{
    AtomicU64,
    Ordering,
};
use std::{
    fmt,
    io,
};

use bytes::{
    BufMut,
    Bytes,
    BytesMut,
};
use content_addressing::{
    Codec,
    Digest,
    Hasher,
};
use tokio::fs::{
    self,
    File,
    OpenOptions,
};
use tokio::io::{
    AsyncReadExt,
    AsyncSeekExt,
    AsyncWriteExt,
};
use tokio::sync::RwLock;
use tokio::task;

#[cfg(test)]
mod tests;

/// One append-only file's identity: `{id:020}.log` in `Bitcask`'s
/// directory. A segment is created empty, then appended to sequentially
/// while it's the active one; once rotated out it never changes again
/// until `finish` deletes it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Segment(u64);

impl Segment {
    const FIRST: Segment = Segment(0);

    fn next(self) -> Segment {
        Segment(self.0 + 1)
    }
}

impl fmt::Display for Segment {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:020}", self.0)
    }
}

#[derive(Clone, Copy)]
struct Location {
    /// Which segment the value lives in.
    segment: Segment,
    /// Byte offset of the value within that segment's file.
    offset:  u64,
    /// How many bytes the value is.
    length:  u32,
}

struct Active {
    /// This segment's id.
    id:     Segment,
    /// Open for appending.
    file:   File,
    /// Bytes written to `file` so far.
    offset: u64,
}

struct Inner {
    /// The segment currently being appended to.
    active:  Active,
    /// Segments rotated out, not yet `finish`ed.
    pending: Vec<Segment>,
    /// Every staged key's location, across `active` and `pending`.
    index:   HashMap<Digest, Location>,
}

pub(crate) struct Bitcask {
    /// The directory segments live in.
    dir:        PathBuf,
    inner:      RwLock<Inner>,
    /// Mirrors `inner.active.offset`, readable without locking.
    active_len: AtomicU64,
}

impl Bitcask {
    /// Opens the staging directory at `dir`, creating it if needed, and
    /// replays whatever segments are already there. `codec` decodes each
    /// replayed record to verify it against its own key; it must match
    /// whatever `Cas` itself uses, since a value's digest is only
    /// meaningful once decoded back to its original content.
    pub(crate) async fn open(dir: impl Into<PathBuf>, codec: Codec) -> io::Result<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir).await?;

        let mut ids = Vec::new();
        let mut read_dir = fs::read_dir(&dir).await?;
        while let Some(entry) = read_dir.next_entry().await? {
            if let Some(id) = parse_segment_name(&entry.file_name()) {
                ids.push(id);
            }
        }
        ids.sort_unstable();

        let mut index = HashMap::new();
        let mut pending = Vec::new();
        for (i, &segment) in ids.iter().enumerate() {
            let path = segment_path(&dir, segment);
            let buf = read_bytes(&path).await?;
            let len = buf.len() as u64;

            let verify = (i + 1 == ids.len()).then_some(codec);
            let (valid_len, records) = {
                let buf = buf.clone();
                task::spawn_blocking(move || parse(&buf, verify))
                    .await
                    .expect("parse should not panic")
            };
            if valid_len < len {
                fs::OpenOptions::new().write(true).open(&path).await?.set_len(valid_len).await?;
            }
            // A file with no valid records is either the empty active
            // segment a previous run's `rotate` created but never wrote
            // to, or one torn so badly by a crash that not even its
            // first record survived. Either way, there's nothing to
            // pend; clean it up rather than tracking an empty segment
            // forever.
            if records.is_empty() {
                fs::remove_file(&path).await?;
                continue;
            }
            pending.push(segment);
            for (key, offset, length) in records {
                index.insert(key, Location { segment, offset, length });
            }
        }

        let next = ids.last().map_or(Segment::FIRST, |id| id.next());
        let file = create_segment(&dir, next).await?;

        Ok(Self {
            dir,
            active_len: AtomicU64::new(0),
            inner: RwLock::new(Inner {
                active: Active { id: next, file, offset: 0 },
                pending,
                index,
            }),
        })
    }

    /// Durably appends `value` under `key` to the active segment. Once
    /// this returns, `value` survives a crash of this node.
    pub(crate) async fn put(&self, key: Digest, value: Bytes) -> io::Result<()> {
        let mut inner = self.inner.write().await;
        let record = encode_record(key, &value);
        let offset = inner.active.offset;
        inner.active.file.write_all(&record).await?;
        inner.active.file.sync_all().await?;
        inner.active.offset += record.len() as u64;

        let segment = inner.active.id;
        inner.index.insert(
            key,
            Location { segment, offset: offset + RECORD_HEADER_LEN, length: value.len() as u32 },
        );
        self.active_len.store(inner.active.offset, Ordering::Relaxed);
        Ok(())
    }

    /// Fetches the value staged under `key`, if any.
    pub(crate) async fn get(&self, key: Digest) -> io::Result<Option<Bytes>> {
        let Some(location) = self.inner.read().await.index.get(&key).copied() else {
            return Ok(None);
        };
        let mut file = File::open(self.segment_path(location.segment)).await?;
        file.seek(io::SeekFrom::Start(location.offset)).await?;
        let mut buf = vec![0u8; location.length as usize];
        file.read_exact(&mut buf).await?;
        Ok(Some(Bytes::from(buf)))
    }

    /// Whether `key` is currently staged, without reading its value.
    pub(crate) async fn contains(&self, key: Digest) -> bool {
        self.inner.read().await.index.contains_key(&key)
    }

    /// How many bytes the active segment currently holds. Lock-free, so
    /// `Cas::put_blob` can check it against `Flushing`'s threshold on
    /// every call without contending with concurrent `put`s.
    pub(crate) fn active_len(&self) -> u64 {
        self.active_len.load(Ordering::Relaxed)
    }

    /// Closes the active segment out as a new pending segment and starts
    /// a fresh one. Returns the segment that was just closed out, for
    /// `entries`/`finish`.
    pub(crate) async fn rotate(&self) -> io::Result<Segment> {
        let mut inner = self.inner.write().await;
        let old = inner.active.id;
        let next = old.next();
        let file = create_segment(&self.dir, next).await?;
        inner.active = Active { id: next, file, offset: 0 };
        inner.pending.push(old);
        self.active_len.store(0, Ordering::Relaxed);
        Ok(old)
    }

    /// Segments rotated out but not yet `finish`ed, oldest first. Covers
    /// both a previous flush that failed partway and, after a restart,
    /// whatever `open` found already on disk.
    pub(crate) async fn pending_segments(&self) -> Vec<Segment> {
        self.inner.read().await.pending.clone()
    }

    /// Every `(key, value)` pair in `segment`, in the order they were
    /// written. `segment` must have come from `rotate` or
    /// `pending_segments`; it is never the active segment.
    pub(crate) async fn entries(&self, segment: Segment) -> io::Result<Vec<(Digest, Bytes)>> {
        let buf = read_bytes(&self.segment_path(segment)).await?;
        let records = {
            let buf = buf.clone();
            task::spawn_blocking(move || parse(&buf, None).1).await.expect("parse should not panic")
        };
        Ok(records
            .into_iter()
            .map(|(key, offset, length)| {
                (key, buf.slice(offset as usize..(offset + u64::from(length)) as usize))
            })
            .collect())
    }

    /// Deletes `segment` and evicts its entries from the index. Only
    /// call this once `segment`'s content is durably packed elsewhere.
    pub(crate) async fn finish(&self, segment: Segment) -> io::Result<()> {
        {
            let mut inner = self.inner.write().await;
            inner.pending.retain(|&id| id != segment);
            inner.index.retain(|_, location| location.segment != segment);
        }
        fs::remove_file(self.segment_path(segment)).await
    }

    fn segment_path(&self, segment: Segment) -> PathBuf {
        segment_path(&self.dir, segment)
    }
}

const RECORD_HEADER_LEN: u64 = 32 + 4;

fn encode_record(key: Digest, value: &Bytes) -> Bytes {
    let mut buf = BytesMut::with_capacity(RECORD_HEADER_LEN as usize + value.len());
    buf.extend_from_slice(key.as_ref());
    buf.put_u32(value.len() as u32);
    buf.extend_from_slice(value);
    buf.freeze()
}

/// Parses records from `buf` in order, returning each one's key and the
/// byte range its value occupies within `buf`, plus how many bytes from
/// the start of `buf` are valid.
///
/// `codec` is `None` for a segment already known to be fully durable, in
/// which case only the length framing is trusted. It's `Some` only for
/// replaying the one segment that could have been active during a crash:
/// each record is then decoded and its digest recomputed against its own
/// key, and the first record that fails this, or that the file simply
/// doesn't have enough bytes left for, is treated as a crash-torn tail.
/// Everything from that point on is dropped rather than trusted.
fn parse(buf: &Bytes, codec: Option<Codec>) -> (u64, Vec<(Digest, u64, u32)>) {
    let mut records = Vec::new();
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
        records.push((key, value_start as u64, length));
        offset = value_end;
    }
    (offset as u64, records)
}

async fn create_segment(dir: &Path, segment: Segment) -> io::Result<File> {
    OpenOptions::new().write(true).create_new(true).open(segment_path(dir, segment)).await
}

async fn read_bytes(path: &Path) -> io::Result<Bytes> {
    fs::read(path).await.map(Bytes::from)
}

fn segment_path(dir: &Path, segment: Segment) -> PathBuf {
    dir.join(format!("{segment}.log"))
}

fn parse_segment_name(name: &OsStr) -> Option<Segment> {
    name.to_str()?.strip_suffix(".log")?.parse().ok().map(Segment)
}
