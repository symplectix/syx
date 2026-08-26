//! The committer task: the only thing that ever writes to a segment
//! file, or rotates the active one out.

use std::io;
use std::path::PathBuf;
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
};
use crossbeam_skiplist::SkipMap;
use tokio::sync::{
    RwLock,
    mpsc,
    oneshot,
};

use super::{
    FileId,
    Pending,
    Records,
    Segment,
    Slot,
    file_id,
    revalidate_segment,
};

/// A record's on-disk header: a 32-byte digest, then a big-endian u32
/// value length. `commit` is what actually writes this layout; `parse`
/// (in `forgetter`) just has to keep reading it the same way.
pub(super) const RECORD_HEADER_LEN: u64 = 32 + 4;

/// What `spawn` hands back to `Forgetter`: everything it needs to observe
/// and drive a running committer, without exposing the committer itself.
pub(super) struct Handle {
    commands: mpsc::UnboundedSender<Command>,
    active: Arc<RwLock<Active>>,
    active_segment_len: Arc<AtomicU64>,
}

/// The active segment with every record committed to it so far.
struct Active {
    segment: Segment,
    records: Records,
}

/// A request handed to the committer task over its `commands` channel.
/// Private: `Handle` is the only thing that ever sends one, and
/// `Committer` the only thing that ever receives one.
enum Command {
    Put(Put),
    Rotate(Rotate),
}

/// One `put` waiting on the committer.
struct Put {
    key:   Digest,
    value: Bytes,
    /// Replied with the active segment's length once this `put`'s bytes
    /// have landed in it.
    reply: oneshot::Sender<io::Result<u64>>,
}

/// One `rotate` waiting on the committer.
struct Rotate {
    reply: oneshot::Sender<io::Result<FileId>>,
}

impl Handle {
    /// How many bytes the active segment currently holds.
    pub(super) fn active_segment_len(&self) -> u64 {
        self.active_segment_len.load(Ordering::Relaxed)
    }

    /// Fetches the value staged under `key` in the active segment, if
    /// any. Knows nothing about `pending`; `Forgetter::get` walks that
    /// itself once this comes back empty.
    pub(super) async fn get(&self, key: Digest) -> io::Result<Option<Bytes>> {
        let found = {
            let active = self.active.read().await;
            active.records.get(&key).map(|slot| (active.segment.clone(), *slot))
        };
        match found {
            Some((segment, slot)) => segment.bytes(slot).await.map(Some),
            None => Ok(None),
        }
    }

    /// Whether `key` is staged in the active segment. Knows nothing
    /// about `pending`; `Forgetter::contains` checks that itself once
    /// this is `false`.
    pub(super) async fn contains(&self, key: Digest) -> bool {
        self.active.read().await.records.contains_key(&key)
    }

    /// Durably appends `value` under `key` to the active segment, and
    /// returns its length immediately after.
    pub(super) async fn put(&self, key: Digest, value: Bytes) -> io::Result<u64> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(Command::Put(Put { key, value, reply }))
            .map_err(|_| io::Error::other("forgetter: committer task is gone"))?;
        response.await.map_err(|_| io::Error::other("forgetter: committer task is gone"))?
    }

    /// Closes the active segment out as a new pending segment and starts
    /// a fresh one, returning the segment that was just closed out.
    pub(super) async fn rotate(&self) -> io::Result<FileId> {
        let (reply, response) = oneshot::channel();
        self.commands
            .send(Command::Rotate(Rotate { reply }))
            .map_err(|_| io::Error::other("forgetter: committer task is gone"))?;
        response.await.map_err(|_| io::Error::other("forgetter: committer task is gone"))?
    }

    /// A clone of the active segment right now. Exists for tests: lets
    /// one capture a `Segment` before rotating, to check that `seal`
    /// reaches it through the shared `mmap` cell rather than a fresh
    /// lookup back into `pending`.
    #[cfg(test)]
    async fn segment(&self) -> Segment {
        self.active.read().await.segment.clone()
    }
}

/// Builds a committer around a fresh active segment, wired to
/// `pending`, spawns it in its own task, and returns the `Handle`
/// `Forgetter` uses to observe and drive it. Kept separate from
/// `Forgetter::open`'s own setup so a future multi-committer `Forgetter`
/// could spin up several of these the same way, each with its own
/// segment and channel.
pub(super) async fn spawn(
    dir: PathBuf,
    codec: Codec,
    pending: Arc<SkipMap<FileId, Pending>>,
) -> io::Result<Handle> {
    let file_id = file_id::next();
    let segment = Segment::create(file_id::path(&dir, file_id)).await?;

    let active_segment_len = Arc::new(AtomicU64::new(0));
    let active =
        Arc::new(RwLock::new(Active { segment: segment.clone(), records: Records::new() }));

    let (tx, commands) = mpsc::unbounded_channel();
    let committer = Committer {
        dir,
        codec,
        file_id,
        active: Arc::clone(&active),
        active_segment_len: Arc::clone(&active_segment_len),
        segment,
        pending,
        commands,
    };
    tokio::spawn(committer.run());
    Ok(Handle { commands: tx, active, active_segment_len })
}

/// Owns the active segment's write side: the only thing that ever
/// writes to a segment file. Many concurrent `put`s can share one
/// `write`+`sync_data` call instead of each paying for its own.
struct Committer {
    dir:   PathBuf,
    /// Verifies a poisoned segment's recovered tail the same way `open`
    /// verifies a crash-torn one; see `poison`.
    codec: Codec,

    /// The active segment's id. `Segment` itself doesn't carry one; see
    /// `Segment`'s own doc for why. Tracked here alongside `segment`. A
    /// fresh replacement comes from `file_id::next`, not by
    /// incrementing this directly.
    file_id: FileId,
    /// The published view of the active segment, shared with `Handle`'s
    /// `get`/`contains`; see `Active`'s own doc.
    active: Arc<RwLock<Active>>,
    /// The active segment's current length, and the next unwritten
    /// offset within it.
    active_segment_len: Arc<AtomicU64>,
    /// The active segment. A private working copy: writes go through
    /// this directly without taking `active`'s lock, so a `put`'s
    /// `append`+`flush` never waits behind a `get`/`contains` reader.
    segment: Segment,

    /// Rotated out, not yet `finish`ed. Structurally can never contain
    /// the active segment, so nothing removing from it needs to guard
    /// against accidentally sealing the one still being written to.
    pending: Arc<SkipMap<FileId, Pending>>,

    /// Where `Handle::put`/`rotate` send their requests.
    commands: mpsc::UnboundedReceiver<Command>,
}

impl Committer {
    /// Caps how many `Put`s the drain loop in `run` gathers into one
    /// batch before forcing a `commit`, even if the channel still has
    /// more queued. The drain loop's `try_recv` calls are synchronous,
    /// with no `.await` between them; without this cap, a sustained
    /// burst of concurrent `put`s could keep it going indefinitely,
    /// never reaching an `.await` point to yield back to the executor.
    /// Large enough that ordinary bursts never come close to it.
    const MAX_BATCH_SIZE: usize = 1024;

    /// Runs until the `Handle` holding the other end of `commands` is
    /// dropped, and with it the `Forgetter` it backs.
    async fn run(mut self) {
        // A `Rotate` peeked while draining a batch.
        let mut carried: Option<Command> = None;
        // Reused across every `Put` batch, `commit` drains it empty
        // (keeping its capacity) rather than consuming it.
        let mut batch: Vec<Put> = Vec::new();
        loop {
            // Each iteration handles exactly one `Command`: a `Rotate`
            // runs by itself, a `Put` drains whatever else is already
            // queued into one batch and commits all of it together,
            // shown below. `carried` is checked before the channel
            // every time, so a `Rotate` peeked ahead of its turn during
            // a drain still runs before anything new is pulled off the
            // channel. Send order holds without a lock to enforce it.
            let command = match carried.take() {
                Some(command) => command,
                None => match self.commands.recv().await {
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
                    // `try_recv` only grabs what's already queued right
                    // now. There's no deliberate delay to let more
                    // arrive, so the same burst of concurrent `put`s can
                    // land in one commit or several depending on
                    // scheduling. That's fine: under sustained load the
                    // next commit's own write keeps the queue filling
                    // while this one runs, so batches converge on their
                    // own without ever adding latency to wait for one.
                    batch.push(first);
                    while batch.len() < Self::MAX_BATCH_SIZE {
                        match self.commands.try_recv() {
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
    /// waiter. The cost of that one `flush` is shared across however
    /// many `put`s happened to be ready when this batch was drained.
    async fn commit(&mut self, batch: &mut Vec<Put>) {
        let mut slots = Vec::with_capacity(batch.len());
        let mut offset = self.active_segment_len.load(Ordering::Relaxed);
        let mut written = Ok(());
        for msg in batch.iter() {
            let value_offset = offset + RECORD_HEADER_LEN;
            offset += RECORD_HEADER_LEN + msg.value.len() as u64;

            let mut header = BytesMut::with_capacity(RECORD_HEADER_LEN as usize);
            header.extend_from_slice(msg.key.as_ref());
            header.put_u32(msg.value.len() as u32);
            // Appended separately from `msg.value`, not concatenated into
            // one buffer first: `Bytes` is contiguous, so concatenating
            // would mean copying `value`, already an owned `Bytes`, into
            // a fresh one just to hand it to `append`. `value.clone()` is
            // just a refcount bump, no copy.
            if let Err(e) = self.segment.append(header.freeze()).await {
                written = Err(e);
                break;
            }
            if let Err(e) = self.segment.append(msg.value.clone()).await {
                written = Err(e);
                break;
            }

            slots.push(Slot { offset: value_offset, length: msg.value.len() as u32 });
        }
        // Only flushes if every append above actually landed; either
        // way, `result` ends up carrying whichever error came first.
        let result = async {
            written?;
            self.segment.flush().await
        }
        .await;

        match result {
            Ok(()) => {
                self.active_segment_len.store(offset, Ordering::Relaxed);
                // Replies go out after the lock is dropped: sending one
                // doesn't touch `active` at all, so holding the lock
                // across it would only make concurrent `get`/`contains`
                // readers wait longer than the update itself needs.
                {
                    let mut active = self.active.write().await;
                    for (msg, &slot) in batch.iter().zip(&slots) {
                        active.records.insert(msg.key, slot);
                    }
                }
                for msg in batch.drain(..) {
                    let _ = msg.reply.send(Ok(offset));
                }
            }
            Err(e) => {
                for msg in batch.drain(..) {
                    let _ = msg.reply.send(Err(io::Error::new(e.kind(), e.to_string())));
                }
                // `O_APPEND` still lands whatever it managed to write
                // even though this batch failed, so `active_segment_len`
                // can no longer be trusted as this segment's true length.
                let _ = self.poison().await;
            }
        }
        // `batch` is reused across calls to amortize its allocation, so
        // an exceptional run up against `MAX_BATCH_SIZE` would otherwise
        // leave it holding that much capacity forever after.
        batch.shrink_to(Self::MAX_BATCH_SIZE);
    }

    /// Seals the active segment into a new pending one and starts a
    /// fresh active segment. Correctly ordered against `commit` for
    /// free: both run inside the same single-consumer loop, so a
    /// `Rotate` can never jump ahead of `Put`s that were sent first.
    async fn rotate(&mut self) -> io::Result<FileId> {
        let old_id = self.file_id;
        let old = self.start_fresh_segment().await?;

        let _ = old.segment.seal();
        self.pending.insert(old_id, Pending { segment: old.segment, records: old.records });
        Ok(old_id)
    }

    /// Handles a `commit` that failed partway. Starts a fresh segment
    /// immediately so later commits don't inherit a stale segment.
    ///
    /// Re-derives, best-effort, what actually landed durably in the
    /// old segment. This is the same recovery `Forgetter::open` runs at
    /// startup, and it publishes whatever of the old segment is valid.
    ///
    /// If that recovery itself fails, it's still on disk.
    /// The next `open` will find and replay it like any other segment.
    async fn poison(&mut self) -> io::Result<()> {
        let old_id = self.file_id;
        // The replaced `Active` is discarded: whatever it believes it
        // holds may not match what's actually durable, which is exactly
        // what `revalidate_segment` below re-derives from disk instead.
        let _ = self.start_fresh_segment().await?;

        let path = file_id::path(&self.dir, old_id);
        if let Some((segment, records)) = revalidate_segment(&path, Some(self.codec)).await? {
            self.pending.insert(old_id, Pending { segment, records });
        }
        Ok(())
    }

    /// Starts a brand new active segment, swapping it (and a fresh,
    /// empty `Active`) in for both `self.segment` and the published
    /// `active`.
    async fn start_fresh_segment(&mut self) -> io::Result<Active> {
        let next = file_id::next();
        let segment = Segment::create(file_id::path(&self.dir, next)).await?;

        self.active_segment_len.store(0, Ordering::Relaxed);
        self.file_id = next;
        self.segment = segment.clone();

        let fresh = Active { segment, records: Records::new() };
        Ok(std::mem::replace(&mut *self.active.write().await, fresh))
    }
}

#[cfg(test)]
mod tests {
    use content_addressing::{
        ContentFlags,
        Hasher,
    };

    use super::*;

    fn encode(payload: &[u8]) -> (Digest, Bytes) {
        let key = Hasher::new().part(payload).digest();
        let value = Codec::new().encode(ContentFlags::empty(), payload.to_vec());
        (key, Bytes::from(value))
    }

    #[tokio::test]
    async fn rotate_seals_a_mapping_a_segment_captured_before_it_can_see() {
        let dir = testing::tempdir();
        let pending = Arc::new(SkipMap::new());
        let handle = spawn(dir.path().to_owned(), Codec::new(), pending).await.unwrap();
        let (key, value) = encode(b"hello");
        handle.put(key, value).await.unwrap();

        // Captured while the segment was still active: no mapping yet.
        let segment = handle.segment().await;
        assert!(!segment.sealed());

        handle.rotate().await.unwrap();

        // The same clone, never re-fetched since, now sees the mapping
        // `seal` established while rotating, proving it's a shared cell
        // doing this and not a fresh lookup back into `pending`.
        assert!(segment.sealed());
    }
}
