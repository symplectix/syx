//! `Cas`'s local staging area: an append-only log written to a private
//! directory, not to `db` or `blobs`. Every `put` fsyncs before returning,
//! so what's staged here survives this node's own crash, unlike
//! `slatedb`'s in-memory WAL buffer. Content is keyed by its own digest,
//! so replaying the same (key, value) pair after a crash, or retrying the
//! same segment after a failed flush, is always a safe no-op.
//!
//! Records are framed as `[key: 32 bytes][value_len: u32 BE][value]`, with
//! no separate checksum. Verifying a segment means decoding each record
//! and recomputing its digest, which is a stronger check than a CRC would
//! be; `revalidate_segment` runs this whenever a segment's tail might be
//! torn, truncating away anything from the first bad record on.
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
//!
//! Each `index` entry carries its own segment file handle, not just a
//! segment id to look up later, so once `put` hands one out, reading it
//! back never depends on that segment still being tracked in `state`.
//! `finish` evicts a segment from `index` before `state`, but the order
//! doesn't matter for correctness either way: a `get` that already read
//! the old `index` entry keeps working (its own `Arc<File>` clone stays
//! valid even after the file is unlinked), and one that reads after
//! eviction just misses cleanly, no different from the key never having
//! been staged.
//!
//! A `commit` that fails partway poisons the segment it was writing to:
//! `O_APPEND` guarantees every successful `write` lands at the file's
//! true end regardless of what this module believes that end is, so a
//! partial write (or one that landed but whose `sync_data` then failed)
//! can leave the file longer than `self.offset` accounts for. Rather
//! than let later commits compute offsets against that stale belief, the
//! committer starts a fresh segment immediately and, best-effort,
//! re-derives the old one's true contents the same way `open` recovers
//! from a crash (`revalidate_segment`, truncating any torn tail) so
//! nothing durably written is silently lost.

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

#[derive(Clone)]
struct Location {
    /// Which segment the value lives in. Only `finish` needs this, to
    /// know which entries to evict when a segment goes away; reads use
    /// `file` directly and never need to ask `state` about it.
    segment: Segment,
    /// The segment's own file, captured at insert time so a read never
    /// races a concurrent `finish` tearing that segment out of `state`.
    file:    Arc<File>,
    /// Byte offset of the value within `file`.
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
    /// Replied with the active segment's length once this `put`'s bytes
    /// have landed in it, so a caller that needs to react to that (e.g.
    /// `Cas::put_blob` checking it against `Flushing`) doesn't need a
    /// separate `active_len` call right after.
    reply: oneshot::Sender<io::Result<u64>>,
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
    /// Verifies a poisoned segment's recovered tail the same way `open`
    /// verifies a crash-torn one; see `poison`.
    codec:      Codec,
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
                file:    Arc::clone(&self.file),
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
                        index.insert(msg.key, location.clone());
                    }
                }
                for msg in batch {
                    let _ = msg.reply.send(Ok(offset));
                }
            }
            Err(e) => {
                for msg in batch {
                    let _ = msg.reply.send(Err(io::Error::new(e.kind(), e.to_string())));
                }
                // See the module doc's last paragraph: this segment's
                // true tail is no longer known from `self.offset` alone.
                let _ = self.poison().await;
            }
        }
    }

    /// Seals the active segment into a new pending one and starts a
    /// fresh active segment. Correctly ordered against `commit` for
    /// free: both run inside the same single-consumer loop, so a
    /// `Rotate` can never jump ahead of `Put`s that were sent first.
    async fn rotate(&mut self) -> io::Result<Segment> {
        let old_id = self.segment;
        let old_file = Arc::clone(&self.file);
        self.start_fresh_segment().await?;

        self.state.write().await.pending.push(Handle { id: old_id, file: old_file });
        Ok(old_id)
    }

    /// Handles a `commit` that failed partway. Starts a fresh segment
    /// immediately so later commits don't inherit a stale `self.offset`,
    /// then, best-effort, re-derives what actually landed durably in the
    /// old one (the same recovery `Staging::open` runs at startup) and
    /// publishes whatever of it is valid. If that recovery itself fails,
    /// the old segment's valid prefix isn't lost -- it's still on disk,
    /// and the next `open` will find and replay it like any other
    /// segment left over from a previous run.
    async fn poison(&mut self) -> io::Result<()> {
        let old_id = self.segment;
        self.start_fresh_segment().await?;

        let path = segment_path(&self.dir, old_id);
        if let Some((file, records)) = revalidate_segment(&path, Some(self.codec)).await? {
            {
                let mut index = self.index.write().await;
                for (key, offset, length) in records {
                    index.insert(
                        key,
                        Location { segment: old_id, file: Arc::clone(&file), offset, length },
                    );
                }
            }
            self.state.write().await.pending.push(Handle { id: old_id, file });
        }
        Ok(())
    }

    /// Starts a brand new active segment. Doesn't touch `state`/`index`
    /// for whatever segment was active before -- callers decide what,
    /// if anything, becomes visible for it.
    async fn start_fresh_segment(&mut self) -> io::Result<()> {
        let next = self.segment.next();
        let file = create_segment(&self.dir, next).await?;

        self.segment = next;
        self.offset = 0;
        self.active_len.store(0, Ordering::Relaxed);
        self.file = Arc::clone(&file);

        self.state.write().await.active = Handle { id: next, file };
        Ok(())
    }
}

pub(crate) struct Staging {
    /// The directory segments live in.
    dir:         PathBuf,
    /// The active segment's current length, published by the committer
    /// after every batch it commits. Lock-free, so `flush_pending` can
    /// check whether anything is staged without contending with
    /// concurrent `put`s. `put` itself doesn't need this: it gets the
    /// same value back directly from the committer.
    active_len:  Arc<AtomicU64>,
    state:       Arc<RwLock<State>>,
    /// Every staged key's location, across every segment. A separate
    /// lock from `state`, so `get`/`contains` never wait behind the
    /// committer's `write`/`sync_data` -- and self-sufficient (each
    /// entry carries its own file handle), so `get` never needs `state`
    /// at all.
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
            // Every earlier file was already durable before this
            // process ever rotated past it; only the one that could
            // have been active when the previous run stopped needs
            // verifying.
            let verify = (i + 1 == ids.len()).then_some(codec);
            let Some((file, records)) = revalidate_segment(&path, verify).await? else {
                continue;
            };
            for (key, offset, length) in records {
                index.insert(key, Location { segment, file: Arc::clone(&file), offset, length });
            }
            pending.push(Handle { id: segment, file });
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
            codec,
            active_len: Arc::clone(&active_len),
            state: Arc::clone(&state),
            index: Arc::clone(&index),
        };
        tokio::spawn(committer.run(rx));

        Ok(Self { dir, active_len, state, index, max_pending, commands })
    }

    /// The file for `segment`. Panics if it isn't tracked: its only
    /// caller (`entries`) only ever asks for one it already knows must
    /// still be open, from `pending_segments`/`rotate`'s own return
    /// value. `get` doesn't call this -- its `Location`s carry their own
    /// file handle, so a read never needs to ask `state` about a segment
    /// at all.
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
    /// this returns, `value` survives a crash of this node. Returns the
    /// active segment's length immediately after, so a caller that needs
    /// that (e.g. to compare against a flush threshold) gets it for free
    /// instead of making a separate `active_len` call.
    ///
    /// Refuses the write if pending segments already number `max_pending`:
    /// at that point `flush_pending` isn't keeping up, and accepting more
    /// would grow local disk usage without bound.
    pub(crate) async fn put(&self, key: Digest, value: Bytes) -> io::Result<u64> {
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
        let Some(location) = self.index.read().await.get(&key).cloned() else {
            return Ok(None);
        };
        let mut buf = vec![0u8; location.length as usize];
        task::spawn_blocking(move || -> io::Result<Vec<u8>> {
            read_exact_at(&location.file, &mut buf, location.offset)?;
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

    /// How many bytes the active segment currently holds. `put` already
    /// returns this for its own caller; this is for querying it
    /// independently of any particular write, e.g. `flush_pending`
    /// checking whether the active segment has anything worth rotating.
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
        self.index.write().await.retain(|_, location| location.segment != segment);
        self.state.write().await.pending.retain(|handle| handle.id != segment);
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
/// which case only the length framing is trusted. It's `Some` whenever
/// the segment's tail might be torn (see `revalidate_segment`): each
/// record is then decoded and its digest recomputed against its own key,
/// and the first record that fails this, or that the file simply doesn't
/// have enough bytes left for, is treated as a torn tail. Everything from
/// that point on is dropped rather than trusted.
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

/// Re-reads the segment file at `path`, verifying and truncating away any
/// torn tail. Used both by `open` (recovering from a crash) and by
/// `Committer::poison` (recovering from a commit that failed partway) --
/// both situations where a segment's recorded length can no longer be
/// trusted and has to be re-derived from the file itself.
///
/// `verify` is `Some` to also recompute and check each record's digest;
/// `None` trusts the length framing alone, for a segment already known to
/// be fully durable (every segment but the one that could have been
/// active, when called from `open`).
///
/// Returns `None` (after deleting the file) if nothing valid remains --
/// either an empty active segment that was created but never written to,
/// or one torn so badly not even its first record survived.
async fn revalidate_segment(
    path: &Path,
    verify: Option<Codec>,
) -> io::Result<Option<(Arc<File>, Vec<(Digest, u64, u32)>)>> {
    let buf = read_bytes(path).await?;
    let len = buf.len() as u64;

    let (valid_len, records) = {
        let buf = buf.clone();
        task::spawn_blocking(move || parse(&buf, verify)).await.expect("parse should not panic")
    };
    if valid_len < len {
        fs::OpenOptions::new().write(true).open(path).await?.set_len(valid_len).await?;
    }
    if records.is_empty() {
        fs::remove_file(path).await?;
        return Ok(None);
    }

    let file = open_shared(path).await?;
    Ok(Some((file, records)))
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
