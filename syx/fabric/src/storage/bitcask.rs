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
//!
//! # Concurrency
//!
//! Writes use position-addressed I/O (`write_at`/Windows' `seek_write`),
//! not a shared cursor, so concurrent `put`s never contend on a lock:
//! each one reserves its own byte range with a single atomic add, then
//! writes into that range independently. `get`/`contains` only ever
//! touch a separate index lock, so they never wait behind a `put`'s
//! `fsync` either. `state` (which segment is active, which are pending)
//! still needs real exclusion, but only `rotate`/`finish` take it as a
//! writer; `put` takes it as a reader for as long as its write is in
//! flight, purely so `rotate` waits for every in-flight `put` against
//! the segment it's about to seal before treating it as immutable.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::File;
use std::path::{
    Path,
    PathBuf,
};
use std::sync::Arc;
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
use tokio::sync::RwLock;
use tokio::{
    fs,
    task,
};

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
    id:   Segment,
    /// Open for appending. Shared, not exclusive: every write targets
    /// its own reserved byte range (see the module doc), so handing out
    /// clones of this `Arc` costs nothing and needs no lock of its own.
    file: Arc<File>,
}

struct Inner {
    /// The segment currently being appended to.
    active:  Active,
    /// Segments rotated out, not yet `finish`ed, each with its own open
    /// handle so `get` never has to reopen a file it's already read
    /// from.
    pending: Vec<(Segment, Arc<File>)>,
}

pub(crate) struct Bitcask {
    /// The directory segments live in.
    dir:         PathBuf,
    /// Structural changes only: which segment is active, which are
    /// pending. `put` holds this as a reader for its whole write, purely
    /// so `rotate` (a writer) waits for every write in flight against
    /// the segment it's about to seal.
    state:       RwLock<Inner>,
    /// Every staged key's location, across `active` and `pending`. A
    /// separate lock from `state`, so `get`/`contains` never wait behind
    /// a `put`'s `fsync`.
    index:       RwLock<HashMap<Digest, Location>>,
    /// The next unwritten offset in the active segment. A `put` reserves
    /// its byte range by adding to this atomically, before it writes
    /// anything, which is also what makes concurrent `put`s lock-free.
    active_len:  AtomicU64,
    /// `put` refuses new writes once `pending` reaches this many segments,
    /// so a persistently failing `flush_pending` bounds local disk usage
    /// instead of growing it without limit.
    max_pending: u16,
}

impl Bitcask {
    /// Opens the staging directory at `dir`, creating it if needed, and
    /// replays whatever segments are already there. `codec` decodes each
    /// replayed record to verify it against its own key; it must match
    /// whatever `Cas` itself uses, since a value's digest is only
    /// meaningful once decoded back to its original content. `max_pending`
    /// is enforced from this point on, even if replay already found more
    /// pending segments than that: `put` refuses further writes until
    /// enough of the backlog clears.
    pub(crate) async fn open(
        dir: impl Into<PathBuf>,
        codec: Codec,
        max_pending: u16,
    ) -> io::Result<Self> {
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
            let file = open_shared(&path).await?;
            pending.push((segment, file));
            for (key, offset, length) in records {
                index.insert(key, Location { segment, offset, length });
            }
        }

        let next = ids.last().map_or(Segment::FIRST, |id| id.next());
        let file = create_segment(&dir, next).await?;

        Ok(Self {
            dir,
            active_len: AtomicU64::new(0),
            max_pending,
            state: RwLock::new(Inner { active: Active { id: next, file }, pending }),
            index: RwLock::new(index),
        })
    }

    /// Durably appends `value` under `key` to the active segment. Once
    /// this returns, `value` survives a crash of this node.
    ///
    /// Refuses the write if `pending` already holds `max_pending`
    /// segments: at that point `flush_pending` isn't keeping up, and
    /// accepting more would grow local disk usage without bound.
    pub(crate) async fn put(&self, key: Digest, value: Bytes) -> io::Result<()> {
        let guard = self.state.read().await;
        if guard.pending.len() >= self.max_pending as usize {
            return Err(io::Error::other(format!(
                "bitcask: {} segments already pending (max {})",
                guard.pending.len(),
                self.max_pending
            )));
        }
        let file = Arc::clone(&guard.active.file);
        let segment = guard.active.id;

        let record = encode_record(key, &value);
        let offset = self.active_len.fetch_add(record.len() as u64, Ordering::SeqCst);

        task::spawn_blocking(move || -> io::Result<()> {
            write_all_at(&file, &record, offset)?;
            file.sync_all()
        })
        .await
        .expect("write should not panic")?;

        // Only released once the write above is durable: `rotate` takes
        // `state` as a writer to seal `guard.active`, so it can't
        // observe this segment as immutable while this `put` still has
        // it open as a reader.
        drop(guard);

        self.index.write().await.insert(
            key,
            Location { segment, offset: offset + RECORD_HEADER_LEN, length: value.len() as u32 },
        );
        Ok(())
    }

    /// Fetches the value staged under `key`, if any.
    pub(crate) async fn get(&self, key: Digest) -> io::Result<Option<Bytes>> {
        let Some(location) = self.index.read().await.get(&key).copied() else {
            return Ok(None);
        };
        let file = {
            let state = self.state.read().await;
            if location.segment == state.active.id {
                Arc::clone(&state.active.file)
            } else {
                let (_, file) = state
                    .pending
                    .iter()
                    .find(|(segment, _)| *segment == location.segment)
                    .expect("an indexed key's segment is always still open");
                Arc::clone(file)
            }
        };
        let mut buf = vec![0u8; location.length as usize];
        task::spawn_blocking(move || -> io::Result<Vec<u8>> {
            read_exact_at(&file, &mut buf, location.offset)?;
            Ok(buf)
        })
        .await
        .expect("read should not panic")
        .map(|buf| Some(Bytes::from(buf)))
    }

    /// Whether `key` is currently staged, without reading its value.
    pub(crate) async fn contains(&self, key: Digest) -> bool {
        self.index.read().await.contains_key(&key)
    }

    /// How many bytes the active segment currently holds. Lock-free, so
    /// `Cas::put_blob` can check it against `Flushing`'s threshold on
    /// every call without contending with concurrent `put`s.
    pub(crate) fn active_len(&self) -> u64 {
        self.active_len.load(Ordering::Relaxed)
    }

    /// Closes the active segment out as a new pending segment and starts
    /// a fresh one. Returns the segment that was just closed out, for
    /// `entries`/`finish`. Waits for every `put` already in flight
    /// against that segment before treating it as sealed.
    pub(crate) async fn rotate(&self) -> io::Result<Segment> {
        let mut inner = self.state.write().await;
        let old = inner.active.id;
        let old_file = Arc::clone(&inner.active.file);
        let next = old.next();
        let file = create_segment(&self.dir, next).await?;
        inner.active = Active { id: next, file };
        inner.pending.push((old, old_file));
        self.active_len.store(0, Ordering::Relaxed);
        Ok(old)
    }

    /// Segments rotated out but not yet `finish`ed, oldest first. Covers
    /// both a previous flush that failed partway and, after a restart,
    /// whatever `open` found already on disk.
    pub(crate) async fn pending_segments(&self) -> Vec<Segment> {
        self.state.read().await.pending.iter().map(|(segment, _)| *segment).collect()
    }

    /// Every `(key, value)` pair in `segment`, in the order they were
    /// written. `segment` must have come from `rotate` or
    /// `pending_segments`; it is never the active segment.
    pub(crate) async fn entries(&self, segment: Segment) -> io::Result<Vec<(Digest, Bytes)>> {
        let file = {
            let state = self.state.read().await;
            let (_, file) = state
                .pending
                .iter()
                .find(|(s, _)| *s == segment)
                .expect("entries is only ever called on a pending segment");
            Arc::clone(file)
        };
        let buf = task::spawn_blocking(move || -> io::Result<Bytes> {
            let len = file.metadata()?.len() as usize;
            let mut buf = vec![0u8; len];
            read_exact_at(&file, &mut buf, 0)?;
            Ok(Bytes::from(buf))
        })
        .await
        .expect("read should not panic")?;

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
            let mut state = self.state.write().await;
            state.pending.retain(|(id, _)| *id != segment);
        }
        self.index.write().await.retain(|_, location| location.segment != segment);
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

#[cfg(unix)]
fn write_at(file: &File, buf: &[u8], offset: u64) -> io::Result<usize> {
    std::os::unix::fs::FileExt::write_at(file, buf, offset)
}
#[cfg(windows)]
fn write_at(file: &File, buf: &[u8], offset: u64) -> io::Result<usize> {
    std::os::windows::fs::FileExt::seek_write(file, buf, offset)
}

#[cfg(unix)]
fn read_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    std::os::unix::fs::FileExt::read_at(file, buf, offset)
}
#[cfg(windows)]
fn read_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    std::os::windows::fs::FileExt::seek_read(file, buf, offset)
}

/// Writes `buf` to `file` starting at `offset`, retrying on a short
/// write. Never touches a shared cursor, so this is safe to call
/// concurrently from multiple tasks against the same `file`, as long as
/// their `[offset, offset + buf.len())` ranges don't overlap.
fn write_all_at(file: &File, mut buf: &[u8], mut offset: u64) -> io::Result<()> {
    while !buf.is_empty() {
        let n = write_at(file, buf, offset)?;
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::WriteZero, "failed to write whole record"));
        }
        buf = &buf[n..];
        offset += n as u64;
    }
    Ok(())
}

/// Reads exactly `buf.len()` bytes from `file` starting at `offset`,
/// retrying on a short read. Same concurrency property as
/// [`write_all_at`]: safe to call from multiple tasks at once.
fn read_exact_at(file: &File, mut buf: &mut [u8], mut offset: u64) -> io::Result<()> {
    while !buf.is_empty() {
        let n = read_at(file, buf, offset)?;
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

async fn create_segment(dir: &Path, segment: Segment) -> io::Result<Arc<File>> {
    let std_opts = {
        let mut opts = std::fs::OpenOptions::new();
        opts.read(true).write(true).create_new(true);
        opts
    };
    let path = segment_path(dir, segment);
    task::spawn_blocking(move || std_opts.open(path).map(Arc::new))
        .await
        .expect("open should not panic")
}

async fn open_shared(path: &Path) -> io::Result<Arc<File>> {
    let path = path.to_owned();
    task::spawn_blocking(move || std::fs::File::open(path).map(Arc::new))
        .await
        .expect("open should not panic")
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
