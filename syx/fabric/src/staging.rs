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
//! Each `index` entry carries its own clone of the `Segment` it lives in,
//! rather than just an id to look up later, so once `put` hands one out,
//! reading it back never depends on that segment still being tracked in
//! `pending`. `finish` evicts a segment from `pending` before `index`,
//! but the order doesn't matter for correctness either way: a `get` that
//! already read the old `index` entry keeps working (its `Segment`
//! clone's `File` stays valid even after the underlying path is
//! unlinked), and one that reads after eviction just misses cleanly, no
//! different from the key never having been staged.
//!
//! The active segment itself is never published anywhere: it lives only
//! in the committer's own `Committer::segment`, since nothing outside
//! needs a `Segment` for it specifically (`find`'s only caller never
//! asks for it, and `get`/`contains` read through `index`'s own captured
//! clones instead). The one fact about it callers do need, its current
//! length, is published separately through `active_len`, a plain atomic.
//! `pending` is the only thing actually shared with the committer, and
//! it's a lock-free `SkipMap` keyed by `FileId` rather than a lock,
//! since what it needs is real concurrent insert/remove/lookup by id,
//! not just readers not blocking a single writer.
//!
//! `get` starts out reading via `read_at`, the same as while the segment
//! is still active, but switches to slicing a mapped view once one
//! exists (see `Segment`'s `mmap` field) -- established once, by `seal`,
//! at the moment a segment is sealed into `pending`, and from then on
//! shared by every clone of that `Segment` value, including ones `index`
//! already held from before sealing happened.
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
use std::io;
use std::path::{
    Path,
    PathBuf,
};
use std::sync::Arc;
use std::sync::atomic::{
    AtomicU64,
    Ordering,
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
use crossbeam_skiplist::SkipMap;
use tokio::sync::{
    RwLock,
    mpsc,
    oneshot,
};
use tokio::{
    fs,
    task,
};

mod segment;

pub(crate) use segment::FileId;
use segment::{
    Segment,
    parse_segment_name,
    segment_path,
};

#[cfg(test)]
mod tests;

#[derive(Clone)]
struct Location {
    /// The segment the value lives in, cloned at insert time from
    /// whatever `Segment` was current then (`commit`, `poison`, or
    /// `open`'s replay). Reading through this never needs to ask
    /// `pending` about it, so a read never races a concurrent `finish`
    /// tearing the segment out.
    segment: Segment,
    /// Byte offset of the value within `segment.file`.
    offset:  u64,
    /// How many bytes the value is.
    length:  u32,
}

/// One record's key and where its value lives within a segment's bytes:
/// `offset`/`length` point past the `[key][len]` header, at the value.
#[derive(Clone, Copy)]
pub(crate) struct Record {
    pub(crate) key:    Digest,
    pub(crate) offset: u64,
    pub(crate) length: u32,
}

/// One entry of `pending`: a rotated-out segment together with every
/// record it holds. Kept separate from `Segment` itself, which stays
/// agnostic to record framing (that's `parse`'s job) and which also
/// stands alone as the active segment and inside every `Location` --
/// neither has any use for a whole segment's key list, so it isn't a
/// field of `Segment`.
///
/// `records` is exactly what `finish` needs to evict this segment's
/// keys from `index` in one pass, and what `flush_segments` needs to
/// build a pack's `Entry`s, without either re-parsing `segment`.
#[derive(Clone)]
struct Pending {
    segment: Segment,
    records: Vec<Record>,
}

/// A request handed to the committer task over its `commands` channel.
enum Command {
    Put(PutMsg),
    Rotate(RotateMsg),
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
    reply: oneshot::Sender<io::Result<FileId>>,
}

/// Owns the active segment's write side -- the only thing that ever
/// writes to a segment file. Every `Staging::put`/`rotate` funnels
/// through its `commands` channel instead of writing directly, so many
/// concurrent `put`s can share one `write`+`sync_data` call instead of
/// each paying for its own (see the module doc's "Concurrency" section).
struct Committer {
    dir:        PathBuf,
    /// The active segment. Only the committer itself ever reads or
    /// writes it -- nothing outside needs a `Segment` for the active
    /// one specifically: `find`'s only caller never asks for it (see
    /// its own doc), and `get`/`contains` read through `index`'s own
    /// captured `Segment` clones instead. `Staging::active_len` covers
    /// the one thing about the active segment external callers do need.
    segment:    Segment,
    /// The next unwritten offset in the active segment. Only the
    /// committer itself ever advances this; `Staging::active_len` reads
    /// the published copy in `active_len` instead.
    offset:     u64,
    /// Verifies a poisoned segment's recovered tail the same way `open`
    /// verifies a crash-torn one; see `poison`.
    codec:      Codec,
    active_len: Arc<AtomicU64>,
    /// Every record `commit` has written to the active segment so far,
    /// in order. Handed off to `pending` as `Pending::records` by
    /// `rotate` (or replaced outright by `poison`'s own re-derived
    /// records); reset by `start_fresh_segment` either way.
    records:    Vec<Record>,
    /// Rotated out, not yet `finish`ed. Structurally can never contain
    /// the active segment, so nothing removing from it needs to guard
    /// against accidentally sealing the one still being written to.
    /// Keyed and ordered by `FileId`, so oldest-first iteration (see
    /// `Staging::pending_segments`) is a fact of the type, not a
    /// convention `rotate`/`poison` have to uphold by always inserting
    /// at the end.
    pending:    Arc<SkipMap<FileId, Pending>>,
    index:      Arc<RwLock<HashMap<Digest, Location>>>,
}

impl Committer {
    /// Runs until every `Staging` handle (and thus every `commands`
    /// sender) is dropped.
    async fn run(mut self, mut commands: mpsc::UnboundedReceiver<Command>) {
        // A `Rotate` peeked while draining a batch.
        let mut carried: Option<Command> = None;
        // Reused across every `Put` batch, `commit` drains it empty
        // (keeping its capacity) rather than consuming it.
        let mut batch: Vec<PutMsg> = Vec::new();
        loop {
            // Each iteration handles exactly one `Command`: a `Rotate`
            // runs by itself, a `Put` drains whatever else is already
            // queued into one batch and commits all of it together
            // (below). `carried` is checked before the channel every
            // time, so a `Rotate` peeked ahead of its turn during a
            // drain still runs before anything new is pulled off the
            // channel -- send order holds without a lock to enforce it.
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
                    // `try_recv` only grabs what's already queued, right
                    // now -- there's no deliberate delay to let more
                    // arrive, so the same burst of concurrent `put`s can
                    // land in one commit or several depending on
                    // scheduling. That's fine: under sustained load the
                    // next commit's own write keeps the queue filling
                    // while this one runs, so batches converge on their
                    // own without ever adding latency to wait for one.
                    batch.push(first);
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
                    self.commit(&mut batch).await;
                }
            }
        }
    }

    /// Appends every record in `batch`, then syncs once with a single
    /// `flush` call, and publishes their locations and replies to every
    /// waiter. The group commit itself: `write` is cheap (it only lands
    /// in the page cache), so nothing is gained batching it into one
    /// call, but the cost of one `fsync`-equivalent `flush` is shared
    /// across however many `put`s happened to be ready when this batch
    /// was drained.
    async fn commit(&mut self, batch: &mut Vec<PutMsg>) {
        let mut locations = Vec::with_capacity(batch.len());
        let mut offset = self.offset;
        let mut written = Ok(());
        for msg in batch.iter() {
            let value_offset = offset + RECORD_HEADER_LEN;
            offset += RECORD_HEADER_LEN + msg.value.len() as u64;

            let mut header = BytesMut::with_capacity(RECORD_HEADER_LEN as usize);
            header.extend_from_slice(msg.key.as_ref());
            header.put_u32(msg.value.len() as u32);
            // Appended separately from `msg.value`, not concatenated into
            // one buffer first: `Bytes` is contiguous, so concatenating
            // would mean copying `value` (already an owned `Bytes`) into a
            // fresh one just to hand it to `append`. `value.clone()` is
            // just a refcount bump, no copy.
            if let Err(e) = self.segment.append(header.freeze()).await {
                written = Err(e);
                break;
            }
            if let Err(e) = self.segment.append(msg.value.clone()).await {
                written = Err(e);
                break;
            }

            locations.push(Location {
                segment: self.segment.clone(),
                offset:  value_offset,
                length:  msg.value.len() as u32,
            });
        }
        let result = match written {
            Ok(()) => self.segment.flush().await,
            Err(e) => Err(e),
        };

        match result {
            Ok(()) => {
                self.offset = offset;
                self.active_len.store(offset, Ordering::Relaxed);
                let mut index = self.index.write().await;
                for (msg, location) in batch.drain(..).zip(locations) {
                    self.records.push(Record {
                        key:    msg.key,
                        offset: location.offset,
                        length: location.length,
                    });
                    index.insert(msg.key, location);
                    let _ = msg.reply.send(Ok(offset));
                }
            }
            Err(e) => {
                for msg in batch.drain(..) {
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
    async fn rotate(&mut self) -> io::Result<FileId> {
        let old = self.segment.clone();
        let records = std::mem::take(&mut self.records);
        self.start_fresh_segment().await?;

        let old_id = old.id;
        let _ = old.seal();
        self.pending.insert(old_id, Pending { segment: old, records });
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
        let old_id = self.segment.id;
        self.start_fresh_segment().await?;

        let path = segment_path(&self.dir, old_id);
        if let Some((segment, records)) =
            revalidate_segment(old_id, &path, Some(self.codec)).await?
        {
            let _ = segment.seal();
            {
                let mut index = self.index.write().await;
                for &Record { key, offset, length } in &records {
                    index.insert(key, Location { segment: segment.clone(), offset, length });
                }
            }
            self.pending.insert(old_id, Pending { segment, records });
        }
        Ok(())
    }

    /// Starts a brand new active segment. Doesn't touch `pending`/`index`
    /// for whatever segment was active before -- callers decide what,
    /// if anything, becomes visible for it.
    async fn start_fresh_segment(&mut self) -> io::Result<()> {
        let next = self.segment.id.next();
        let segment = Segment::create(next, segment_path(&self.dir, next)).await?;

        self.offset = 0;
        self.active_len.store(0, Ordering::Relaxed);
        self.records.clear();
        self.segment = segment;
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
    /// Rotated out, not yet `finish`ed; see `Committer`'s own field of
    /// the same name.
    pending:     Arc<SkipMap<FileId, Pending>>,
    /// Every staged key's location, across every segment. A separate
    /// lock from `pending`, so `get`/`contains` never wait behind the
    /// committer's `write`/`sync_data` -- and self-sufficient (each
    /// entry carries its own clone of the `Segment` it lives in), so
    /// `get` never needs `pending` at all.
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
        let pending = Arc::new(SkipMap::new());
        for (i, &id) in ids.iter().enumerate() {
            let path = segment_path(&dir, id);
            // Every earlier file was already durable before this
            // process ever rotated past it; only the one that could
            // have been active when the previous run stopped needs
            // verifying.
            let verify = (i + 1 == ids.len()).then_some(codec);
            let Some((segment, records)) = revalidate_segment(id, &path, verify).await? else {
                continue;
            };
            let _ = segment.seal();
            for &Record { key, offset, length } in &records {
                index.insert(key, Location { segment: segment.clone(), offset, length });
            }
            pending.insert(id, Pending { segment, records });
        }

        let next = ids.last().map_or(FileId::FIRST, |id| id.next());
        let segment = Segment::create(next, segment_path(&dir, next)).await?;

        let active_len = Arc::new(AtomicU64::new(0));
        let index = Arc::new(RwLock::new(index));

        let (commands, rx) = mpsc::unbounded_channel();
        let committer = Committer {
            dir: dir.clone(),
            segment,
            offset: 0,
            codec,
            active_len: Arc::clone(&active_len),
            records: Vec::new(),
            pending: Arc::clone(&pending),
            index: Arc::clone(&index),
        };
        tokio::spawn(committer.run(rx));

        Ok(Self { dir, active_len, pending, index, max_pending, commands })
    }

    /// The `Pending` entry for `id`. Panics if it isn't tracked: its
    /// only caller (`segment_bytes`) only ever asks for one it already
    /// knows must still be open, from `pending_segments`/`rotate`'s own
    /// return value -- so this is never asked about the active segment,
    /// only a pending one. `get` doesn't call this either -- its
    /// `Location`s carry their own `Segment` clone, so a read never
    /// needs to ask `pending` about a segment at all.
    fn find(&self, id: FileId) -> Pending {
        self.pending
            .get(&id)
            .map(|entry| entry.value().clone())
            .unwrap_or_else(|| panic!("segment {id} is no longer tracked"))
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
        let pending_len = self.pending.len();
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
        let bytes = location.segment.read(location.offset, location.length).await?;
        Ok(Some(bytes))
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
    pub(crate) async fn rotate(&self) -> io::Result<FileId> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(Command::Rotate(RotateMsg { reply }))
            .map_err(|_| io::Error::other("staging: committer task is gone"))?;
        response.await.map_err(|_| io::Error::other("staging: committer task is gone"))?
    }

    /// Segments rotated out but not yet `finish`ed, oldest first. Covers
    /// both a previous flush that failed partway and, after a restart,
    /// whatever `open` found already on disk.
    pub(crate) fn pending_segments(&self) -> Vec<FileId> {
        self.pending.iter().map(|entry| *entry.key()).collect()
    }

    /// The sealed segment's whole-file bytes, and every record's key,
    /// offset, and length within them, in the order they were written.
    /// `id` must have come from `rotate` or `pending_segments`; it is
    /// never the active segment.
    ///
    /// Just reads `find`'s `Pending`: `records` was already parsed once
    /// (by `commit`, `poison`, or `open`'s replay) when this segment
    /// became pending, so there's nothing left to reparse here.
    ///
    /// For `flush_segments`: a pack object's bytes are exactly a staging
    /// segment's own `[key][len][value]` framing, so the sealed buffer
    /// is what gets uploaded as-is, and each record's offset and length
    /// (pointing past the header, at the value) are exactly what a pack
    /// `Entry` needs -- nothing has to be decoded and reassembled first.
    ///
    /// Reads via mmap, not `read_at`: unlike the active segment, a
    /// pending one is never written to again, so there's no writer to
    /// race and no risk of the file growing out from under the mapping.
    /// Usually reuses the mapping `seal` already established when this
    /// segment was sealed into `pending`; only maps it itself if that
    /// didn't happen or failed.
    pub(crate) async fn segment_bytes(&self, id: FileId) -> io::Result<(Bytes, Vec<Record>)> {
        let pending = self.find(id);
        let buf = pending.segment.seal()?;
        Ok((buf, pending.records))
    }

    /// Every `(key, value)` pair in `id`'s segment, in the order they
    /// were written. Each value is a zero-copy `Bytes` view into
    /// `segment_bytes`'s buffer, so bytes flow from the page cache
    /// straight into whatever the caller does with them without an
    /// intermediate copy into a fresh buffer.
    ///
    /// Test-only: `flush_segments` uses `segment_bytes` directly instead,
    /// since it needs the record offsets/lengths, not decoded values.
    #[cfg(test)]
    pub(crate) async fn entries(&self, id: FileId) -> io::Result<Vec<(Digest, Bytes)>> {
        let (buf, records) = self.segment_bytes(id).await?;
        Ok(records
            .into_iter()
            .map(|Record { key, offset, length }| {
                (key, buf.slice(offset as usize..(offset + u64::from(length)) as usize))
            })
            .collect())
    }

    /// Deletes `id`'s segment and evicts its entries from the index.
    /// Only call this once its content is durably packed elsewhere. `id`
    /// must be pending, never the active segment: `pending` structurally
    /// can't hold that one, so there's nothing to guard against here.
    ///
    /// Evicts by `id`'s own `Pending::records`, not a scan of all of
    /// `index`: cost is proportional to this segment's own key count,
    /// not to how much else happens to be staged across every segment.
    pub(crate) async fn finish(&self, id: FileId) -> io::Result<()> {
        if let Some(pending) = self.pending.remove(&id) {
            let mut index = self.index.write().await;
            for record in &pending.value().records {
                index.remove(&record.key);
            }
        }
        fs::remove_file(self.segment_path(id)).await
    }

    fn segment_path(&self, id: FileId) -> PathBuf {
        segment_path(&self.dir, id)
    }
}

const RECORD_HEADER_LEN: u64 = 32 + 4;

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
fn parse(buf: &Bytes, codec: Option<Codec>) -> (u64, Vec<Record>) {
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
        records.push(Record { key, offset: value_start as u64, length });
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
    id: FileId,
    path: &Path,
    verify: Option<Codec>,
) -> io::Result<Option<(Segment, Vec<Record>)>> {
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

    let segment = Segment::open(id, path.to_owned()).await?;
    Ok(Some((segment, records)))
}

async fn read_bytes(path: &Path) -> io::Result<Bytes> {
    fs::read(path).await.map(Bytes::from)
}
