//! `Cas`'s local staging area: an append-only log written to a private
//! directory, not to `db` or `store`. Every `put` fsyncs before returning,
//! so what's staged here survives this node's own crash, unlike
//! `slatedb`'s in-memory WAL buffer. Content is keyed by its own digest,
//! so replaying the same (key, value) pair after a crash, or retrying the
//! same segment after a failed flush, is always a safe no-op.
//!
//! Records are framed as `[key: 32 bytes][value_len: u32 BE][value]`, with
//! no separate checksum. `Staging::open` verifies the records it replays
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
//! `put` doesn't write directly. It sends its `(key, value)` to a single
//! committer task over an unbounded channel and waits for that task to
//! reply. The committer drains whatever else is already queued before
//! writing, so however many `put`s happen to be ready at once share one
//! `write`+`sync_data` call, instead of each paying for its own -- the
//! same group-commit technique WAL implementations use to amortize
//! `fsync`'s per-call cost under concurrent writers. This matters far
//! more than avoiding write contention: on an SSD there's no seek
//! penalty to dodge in the first place, so the previous position-
//! addressed (`write_at`) design bought little there, while still paying
//! for a separate `fsync`-equivalent call per `put`.
//!
//! Because `put`s and `rotate` funnel through the same single-consumer
//! channel, in the order they were sent, `rotate` can never seal a
//! segment out from under a `put` that was sent before it -- the
//! ordering the committer already needs for its own batching gives this
//! guarantee for free, no separate lock required. `get`/`contains` only
//! ever touch a separate index lock and read via position-addressed I/O
//! (`read_at`/Windows' `seek_read`), so they never wait behind the
//! committer's `write`/`sync_data` either.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::File;
use std::io::Write as _;
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
use tokio::sync::{
    RwLock,
    mpsc,
    oneshot,
};
use tokio::{
    fs,
    task,
};

#[cfg(test)]
mod tests;

/// One append-only file's identity: `{id:020}.log` in `Staging`'s
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

/// A segment's open file, whether it's the one currently being appended
/// to or one already rotated out. Cheap to clone (just bumps the `Arc`):
/// the committer task is the only writer, so handing a clone to a reader
/// costs nothing and needs no lock of its own -- positioned reads
/// (`read_at`) never contend with the committer's own writes.
struct Handle {
    id:   Segment,
    file: Arc<File>,
}

/// Which segment is active and which are pending. Plain data, other than
/// the committer's own writes (see `Committer::rotate`), nothing mutates
/// this; readers just take a shared lock.
struct State {
    active:  Handle,
    /// Rotated out, not yet `finish`ed. Structurally can never contain
    /// the active segment, so nothing removing from it needs to guard
    /// against accidentally sealing the one still being written to.
    pending: Vec<Handle>,
}

/// One `put` waiting on the committer.
struct PutMsg {
    key:   Digest,
    value: Bytes,
    reply: oneshot::Sender<io::Result<()>>,
}

/// One `rotate` waiting on the committer.
struct RotateMsg {
    reply: oneshot::Sender<io::Result<Segment>>,
}

/// A request handed to the committer task over its `commands` channel.
enum Command {
    Put(PutMsg),
    Rotate(RotateMsg),
}

/// Owns the active segment's write side -- the only thing that ever
/// writes to a segment file. Every `Staging::put`/`rotate` funnels
/// through its `commands` channel instead of writing directly, so many
/// concurrent `put`s can share one `write`+`sync_data` call instead of
/// each paying for its own (see the module doc's "Concurrency" section).
struct Committer {
    dir:        PathBuf,
    file:       Arc<File>,
    segment:    Segment,
    /// The next unwritten offset in the active segment. Only the
    /// committer itself ever advances this; `Staging::active_len` reads
    /// the published copy in `active_len` instead.
    offset:     u64,
    active_len: Arc<AtomicU64>,
    state:      Arc<RwLock<State>>,
    index:      Arc<RwLock<HashMap<Digest, Location>>>,
}

impl Committer {
    /// Runs until every `Staging` handle (and thus every `commands`
    /// sender) is dropped.
    async fn run(mut self, mut commands: mpsc::UnboundedReceiver<Command>) {
        // A `Rotate` peeked while draining a batch, but not yet allowed
        // to run, since everything sent before it still needs to land
        // in the segment it's about to seal.
        let mut carried: Option<Command> = None;
        loop {
            let command = match carried.take() {
                Some(command) => command,
                None => match commands.recv().await {
                    Some(command) => command,
                    None => return,
                },
            };

            match command {
                Command::Rotate(msg) => {
                    let result = self.rotate().await;
                    let _ = msg.reply.send(result);
                }
                Command::Put(first) => {
                    let mut batch = vec![first];
                    loop {
                        match commands.try_recv() {
                            Ok(Command::Put(msg)) => batch.push(msg),
                            Ok(rotate @ Command::Rotate(_)) => {
                                carried = Some(rotate);
                                break;
                            }
                            Err(_) => break,
                        }
                    }
                    self.commit(batch).await;
                }
            }
        }
    }

    /// Writes every record in `batch` with one `write`+`sync_data` call,
    /// then publishes their locations and replies to every waiter. The
    /// group commit itself: the cost of one `fsync`-equivalent call is
    /// shared across however many `put`s happened to be ready when this
    /// batch was drained.
    async fn commit(&mut self, batch: Vec<PutMsg>) {
        let mut buf = BytesMut::new();
        let mut locations = Vec::with_capacity(batch.len());
        let mut offset = self.offset;
        for msg in &batch {
            let record = encode_record(msg.key, &msg.value);
            let value_offset = offset + RECORD_HEADER_LEN;
            offset += record.len() as u64;
            buf.extend_from_slice(&record);
            locations.push(Location {
                segment: self.segment,
                offset:  value_offset,
                length:  msg.value.len() as u32,
            });
        }
        let buf = buf.freeze();

        let file = Arc::clone(&self.file);
        let result = task::spawn_blocking(move || -> io::Result<()> {
            let mut w = &*file;
            w.write_all(&buf)?;
            file.sync_data()
        })
        .await
        .expect("write should not panic");

        match result {
            Ok(()) => {
                self.offset = offset;
                self.active_len.store(offset, Ordering::Relaxed);
                {
                    let mut index = self.index.write().await;
                    for (msg, location) in batch.iter().zip(&locations) {
                        index.insert(msg.key, *location);
                    }
                }
                for (msg, _) in batch.into_iter().zip(locations) {
                    let _ = msg.reply.send(Ok(()));
                }
            }
            Err(e) => {
                for msg in batch {
                    let _ = msg.reply.send(Err(io::Error::new(e.kind(), e.to_string())));
                }
            }
        }
    }

    /// Seals the active segment into a new pending one and starts a
    /// fresh active segment. Correctly ordered against `commit` for
    /// free: both run inside the same single-consumer loop, so a
    /// `Rotate` can never jump ahead of `Put`s that were sent first.
    async fn rotate(&mut self) -> io::Result<Segment> {
        let next = self.segment.next();
        let file = create_segment(&self.dir, next).await?;
        let old_id = self.segment;
        let old_file = std::mem::replace(&mut self.file, Arc::clone(&file));

        self.segment = next;
        self.offset = 0;
        self.active_len.store(0, Ordering::Relaxed);

        let mut state = self.state.write().await;
        state.pending.push(Handle { id: old_id, file: old_file });
        state.active = Handle { id: next, file };
        Ok(old_id)
    }
}

pub(crate) struct Staging {
    /// The directory segments live in.
    dir:         PathBuf,
    /// The active segment's current length, published by the committer
    /// after every batch it commits. Lock-free, so `Cas::put_blob` can
    /// check it against `Flushing`'s threshold on every call without
    /// contending with concurrent `put`s.
    active_len:  Arc<AtomicU64>,
    state:       Arc<RwLock<State>>,
    /// Every staged key's location, across every segment. A separate
    /// lock from `state`, so `get`/`contains` never wait behind the
    /// committer's `write`/`sync_data`.
    index:       Arc<RwLock<HashMap<Digest, Location>>>,
    /// `put` refuses new writes once pending segments reach this many,
    /// so a persistently failing `flush_pending` bounds local disk usage
    /// instead of growing it without limit.
    max_pending: u16,
    /// Where `put`/`rotate` send their requests; the committer task
    /// holds the matching receiver for as long as any `Staging` handle
    /// (and thus any clone of this sender) is still alive.
    commands:    mpsc::UnboundedSender<Command>,
}

impl Staging {
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
            pending.push(Handle { id: segment, file });
            for (key, offset, length) in records {
                index.insert(key, Location { segment, offset, length });
            }
        }

        let next = ids.last().map_or(Segment::FIRST, |id| id.next());
        let file = create_segment(&dir, next).await?;

        let active_len = Arc::new(AtomicU64::new(0));
        let state = Arc::new(RwLock::new(State {
            active: Handle { id: next, file: Arc::clone(&file) },
            pending,
        }));
        let index = Arc::new(RwLock::new(index));

        let (commands, rx) = mpsc::unbounded_channel();
        let committer = Committer {
            dir: dir.clone(),
            file,
            segment: next,
            offset: 0,
            active_len: Arc::clone(&active_len),
            state: Arc::clone(&state),
            index: Arc::clone(&index),
        };
        tokio::spawn(committer.run(rx));

        Ok(Self { dir, active_len, state, index, max_pending, commands })
    }

    /// The file for `segment`. Panics if it isn't tracked: every caller
    /// only ever asks for one it already knows must still be open,
    /// either from `Cas`'s own index or from `pending_segments`/`rotate`'s
    /// own return value.
    async fn find(&self, segment: Segment) -> Arc<File> {
        let state = self.state.read().await;
        if state.active.id == segment {
            return Arc::clone(&state.active.file);
        }
        state
            .pending
            .iter()
            .find(|handle| handle.id == segment)
            .map(|handle| Arc::clone(&handle.file))
            .unwrap_or_else(|| panic!("segment {segment} is no longer tracked"))
    }

    /// Durably appends `value` under `key` to the active segment. Once
    /// this returns, `value` survives a crash of this node.
    ///
    /// Refuses the write if pending segments already number `max_pending`:
    /// at that point `flush_pending` isn't keeping up, and accepting more
    /// would grow local disk usage without bound.
    pub(crate) async fn put(&self, key: Digest, value: Bytes) -> io::Result<()> {
        let pending_len = self.state.read().await.pending.len();
        if pending_len >= self.max_pending as usize {
            return Err(io::Error::other(format!(
                "staging: {pending_len} segments already pending (max {})",
                self.max_pending
            )));
        }

        let (reply, response) = oneshot::channel();
        self.commands
            .send(Command::Put(PutMsg { key, value, reply }))
            .map_err(|_| io::Error::other("staging: committer task is gone"))?;
        response.await.map_err(|_| io::Error::other("staging: committer task is gone"))?
    }

    /// Fetches the value staged under `key`, if any.
    pub(crate) async fn get(&self, key: Digest) -> io::Result<Option<Bytes>> {
        let Some(location) = self.index.read().await.get(&key).copied() else {
            return Ok(None);
        };
        let file = self.find(location.segment).await;
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

    /// How many bytes the active segment currently holds.
    pub(crate) fn active_len(&self) -> u64 {
        self.active_len.load(Ordering::Relaxed)
    }

    /// Closes the active segment out as a new pending segment and starts
    /// a fresh one. Returns the segment that was just closed out, for
    /// `entries`/`finish`.
    pub(crate) async fn rotate(&self) -> io::Result<Segment> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(Command::Rotate(RotateMsg { reply }))
            .map_err(|_| io::Error::other("staging: committer task is gone"))?;
        response.await.map_err(|_| io::Error::other("staging: committer task is gone"))?
    }

    /// Segments rotated out but not yet `finish`ed, oldest first. Covers
    /// both a previous flush that failed partway and, after a restart,
    /// whatever `open` found already on disk.
    pub(crate) async fn pending_segments(&self) -> Vec<Segment> {
        self.state.read().await.pending.iter().map(|handle| handle.id).collect()
    }

    /// Every `(key, value)` pair in `segment`, in the order they were
    /// written. `segment` must have come from `rotate` or
    /// `pending_segments`; it is never the active segment.
    pub(crate) async fn entries(&self, segment: Segment) -> io::Result<Vec<(Digest, Bytes)>> {
        let file = self.find(segment).await;
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
    /// `segment` must be pending, never the active segment: `pending`
    /// structurally can't hold that one, so there's nothing to guard
    /// against here.
    pub(crate) async fn finish(&self, segment: Segment) -> io::Result<()> {
        self.state.write().await.pending.retain(|handle| handle.id != segment);
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
fn read_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    std::os::unix::fs::FileExt::read_at(file, buf, offset)
}
#[cfg(windows)]
fn read_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    std::os::windows::fs::FileExt::seek_read(file, buf, offset)
}

/// Reads exactly `buf.len()` bytes from `file` starting at `offset`,
/// retrying on a short read. Never touches a shared cursor, so this is
/// safe to call concurrently from multiple tasks, including while the
/// committer is appending to the same `file` through its own cursor.
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
        opts.read(true).append(true).create_new(true);
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
