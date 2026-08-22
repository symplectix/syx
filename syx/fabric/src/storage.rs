//! Content-addressed blob storage: chunking, encoding on the way in,
//! decoding and verifying on the way out.
//!
//! # Data layout
//!
//! ## Staging
//!
//! Not-yet-packed blobs are staged in `staging`, a local durable log, not
//! in `db`. `db` only ever holds pointers to already-packed content; see
//! `staging`'s own module doc for why.
//!
//! ## Packing
//!
//! One object per chunk means one backend API call per chunk, which
//! gets expensive as blob count grows. Consolidating many small objects
//! into fewer, larger packs cuts that down.
//!
//! With `cas_prefix` as `"cas/"`:
//!
//! - `cas/sha256/{digest}`: one entry per blob/chunk, keyed by its own 32-byte digest (raw bytes,
//!   not hex; `sha256` names the hashing scheme so a future switch to a different one, e.g.
//!   `blake3`, can live alongside these keys instead of colliding with them). The value is
//!   [`Entry::encode`]'s output: `[pack_id: 32 bytes][offset: u64][length: u64]`. Written once,
//!   when `flush_pending` consolidates a staging segment into a pack.
//!
//! ## Object Store
//!
//! - `cas/sha256/{pack_id:x}`: one object per consolidated segment, hex-encoded. A pack object's
//!   bytes are just its packed entries' concatenated bytes. A `Entry`'s `offset`/`length` say where
//!   its bytes start and how long they run within that concatenation.
use std::io;
use std::ops::Range;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{
    AtomicU32,
    Ordering,
};
use std::time::{
    Duration,
    Instant,
};

use bytes::{
    Buf,
    BufMut,
    Bytes,
    BytesMut,
};
use content_addressing::{
    Chunking,
    Codec,
    ContentFlags,
    Digest,
    FromBytes,
    Hasher,
    ToBytes,
};
use futures::StreamExt as _;
use futures::stream::{
    self,
    FuturesUnordered,
};
use object_store::path::Path;
use object_store::{
    GetOptions,
    ObjectStore,
    ObjectStoreExt as _,
    PutPayload,
};
use tokio::io::{
    AsyncRead,
    AsyncWrite,
    AsyncWriteExt as _,
};
use tokio::sync::OwnedMutexGuard;
use tokio::task;

#[cfg(test)]
mod tests;

use crate::staging::{
    self,
    Staging,
};

fn invalid_data(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

fn other(e: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::other(e)
}

/// The default `db_prefix`, for the common case of `db_backend` existing
/// solely for this `Graph`'s own `db`.
pub(crate) const DEFAULT_DB_PREFIX: &str = "";

/// The default `cas_prefix`, for the common case of `blobs` existing
/// solely for this `Graph`'s own blob storage.
pub(crate) const DEFAULT_CAS_PREFIX: &str = "cas/";

/// The default `flush_threshold`, 32 MiB, enough to consolidate several
/// dozen chunks per pack.
pub(crate) const DEFAULT_FLUSH_THRESHOLD: u64 = Chunking::AVG_SIZE as u64 * 64;

/// The default `max_staging_duration`: bounds how long a blob can stay
/// invisible to every other reader of `Graph` even when write volume
/// never crosses `flush_threshold` on its own.
pub(crate) const DEFAULT_MAX_STAGING_DURATION: Duration = Duration::from_secs(30);

/// The default `max_pending_segments`: bounds `staging`'s local disk
/// usage, at this default roughly `16 * flush_threshold`, to a finite
/// amount even if `flush_pending` fails indefinitely.
pub(crate) const DEFAULT_MAX_PENDING_SEGMENTS: u16 = 16;

/// Flush behavior: when to consolidate staged entries into a pack, and
/// bookkeeping for that process.
#[derive(Clone)]
pub(crate) struct Flushing {
    /// Serializes `flush_pending`.
    mutex: Arc<tokio::sync::Mutex<()>>,
    /// Consecutive `flush_pending` failures, reset to 0 on success.
    /// `put_blob` reads this to decide whether to run the next flush in
    /// the background or wait on it and propagate its error.
    failures: Arc<AtomicU32>,
    /// How many bytes to stage before consolidating into a pack.
    bytes_threshold: u64,
    /// How long to let a blob sit staged, unpacked, before consolidating
    /// regardless of `bytes_threshold`.
    duration_threshold: Duration,
    /// When the active segment currently being staged into started.
    staging_since: Arc<std::sync::Mutex<Instant>>,
}

impl Flushing {
    /// Builds `Flushing` for `Graph` to hold directly. `Graph::Builder`
    /// is the configuration surface, resolving defaults and overrides;
    /// this just builds what it resolves.
    pub(crate) fn new(bytes_threshold: u64, duration_threshold: Duration) -> Self {
        Self {
            mutex: Arc::new(tokio::sync::Mutex::new(())),
            failures: Arc::new(AtomicU32::new(0)),
            bytes_threshold,
            duration_threshold,
            staging_since: Arc::new(std::sync::Mutex::new(Instant::now())),
        }
    }

    /// Whether enough has accumulated since the last flush to make one
    /// due now: `active_len` crossing `bytes_threshold`, or enough time
    /// having passed since `staging_since` regardless of `active_len`.
    fn is_due(&self, active_len: u64) -> bool {
        active_len >= self.bytes_threshold
            || self.staging_since.lock().unwrap().elapsed() >= self.duration_threshold
    }

    /// Restarts the clock `is_due` measures elapsed time against, for
    /// the segment that just became active.
    fn reset_staging_since(&self) {
        *self.staging_since.lock().unwrap() = Instant::now();
    }

    /// Consecutive `flush_pending` failures so far.
    fn failures(&self) -> u32 {
        self.failures.load(Ordering::Relaxed)
    }

    fn record_success(&self) {
        self.failures.store(0, Ordering::Relaxed);
    }

    fn record_failure(&self) {
        self.failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Claims exclusive rights to run a flush right now, or `None` if
    /// another one is already in progress. An owned guard (not tied to
    /// `&self`'s lifetime), so a winning caller can carry it into a
    /// `tokio::spawn`ed task.
    ///
    /// Callers should claim before deciding to spawn or wait on a flush
    /// at all, not just before doing the flush's actual work: group
    /// commit can report the same crossed threshold to every `put_blob`
    /// call that landed in the same batch, and claiming early means only
    /// the one that actually wins bothers spawning anything, instead of
    /// every one of them spawning a task that mostly just loses a race.
    fn try_claim(&self) -> Option<OwnedMutexGuard<()>> {
        self.mutex.clone().try_lock_owned().ok()
    }
}

/// Where a packed blob's bytes live: which pack object, and where within it.
#[derive(Clone, Copy)]
struct Entry {
    pack_id: Digest,
    offset:  u64,
    length:  u64,
}

impl Entry {
    fn encode(self) -> Bytes {
        let mut buf = BytesMut::with_capacity(32 + 8 + 8);
        buf.extend_from_slice(self.pack_id.as_ref());
        buf.extend_from_slice(&self.offset.to_be_bytes());
        buf.extend_from_slice(&self.length.to_be_bytes());
        buf.freeze()
    }

    fn decode(bytes: &Bytes) -> io::Result<Self> {
        if bytes.len() != 32 + 8 + 8 {
            return Err(invalid_data("malformed Entry"));
        }
        let mut pack_id = [0u8; 32];
        pack_id.copy_from_slice(&bytes[0..32]);
        let offset = u64::from_be_bytes(bytes[32..40].try_into().unwrap());
        let length = u64::from_be_bytes(bytes[40..48].try_into().unwrap());
        Ok(Entry { pack_id: Digest::new(pack_id), offset, length })
    }
}

/// The blob-storage facet of a `Graph`: chunking, encoding/decoding, and
/// physical storage of blobs, addressed by digest.
/// A borrowed view, not an owned type. Construct one fresh per call via
/// `Graph::cas()` rather than holding onto one.
#[derive(Clone, Copy)]
pub struct Cas<'a> {
    db:         &'a slatedb::Db,
    blobs:      &'a Arc<dyn ObjectStore>,
    staging:    &'a Arc<Staging>,
    cas_prefix: &'a str,
    flushing:   &'a Flushing,
    chunking:   Chunking,
    codec:      Codec,
}

impl<'a> Cas<'a> {
    /// How many consecutive `flush_pending` failures `put_blob` tolerates
    /// before switching from running it in the background to waiting on
    /// it and propagating its error.
    const MAX_CONSECUTIVE_FLUSH_FAILURES: u32 = 3;

    /// How many chunks `copy_from`/`read_into` keep in flight at once.
    /// A large blob's chunks would otherwise be staged or fetched one at
    /// a time, each fully awaited before the next starts: group commit
    /// only batches writes that happen to be in flight together, so a
    /// single caller awaiting its own chunks serially never benefits
    /// from it. Bounded rather than unbounded, since each in-flight
    /// chunk holds up to `Chunking::MAX_SIZE` bytes in memory.
    const MAX_CONCURRENT_CHUNKS: usize = 8;

    /// Only `Graph::cas()` calls this.
    pub(crate) fn new(
        db: &'a slatedb::Db,
        blobs: &'a Arc<dyn ObjectStore>,
        staging: &'a Arc<Staging>,
        cas_prefix: &'a str,
        flushing: &'a Flushing,
        chunking: Chunking,
        codec: Codec,
    ) -> Self {
        Self { db, blobs, staging, cas_prefix, flushing, chunking, codec }
    }

    fn entry_key(&self, key: Digest) -> Vec<u8> {
        entry_key(self.cas_prefix, key)
    }

    /// Test-only: mirrors `entry_key`'s prefix, for a range scan over
    /// every entry ever packed.
    #[cfg(test)]
    fn entry_prefix(&self) -> Vec<u8> {
        format!("{}sha256/", self.cas_prefix).into_bytes()
    }

    /// How many distinct keys have ever been packed under this store's
    /// prefix. Test-only.
    #[cfg(test)]
    pub(crate) async fn entry_count(&self) -> io::Result<usize> {
        let mut iter = self.db.scan_prefix(self.entry_prefix(), ..).await.map_err(other)?;
        let mut n = 0;
        while iter.next().await.map_err(other)?.is_some() {
            n += 1;
        }
        Ok(n)
    }

    /// Fetch and decode the entry stored under `key`, if present.
    async fn get_entry(&self, key: Digest) -> io::Result<Option<Entry>> {
        let Some(raw) = self.db.get(self.entry_key(key)).await.map_err(other)? else {
            return Ok(None);
        };
        Entry::decode(&raw).map(Some)
    }

    fn pack_path(&self, pack_id: Digest) -> Path {
        pack_path(self.cas_prefix, pack_id)
    }

    /// Fetch `length` bytes at `offset` from pack `pack_id`.
    async fn get_range(&self, pack_id: Digest, offset: u64, length: u64) -> io::Result<Bytes> {
        let range: Range<u64> = offset..offset + length;
        let opts = GetOptions { range: Some(range.into()), ..Default::default() };
        let result =
            self.blobs.get_opts(&self.pack_path(pack_id), opts).await.map_err(io::Error::from)?;
        Ok(result.bytes().await?)
    }

    /// Whether `key` is already stored, without fetching its value.
    async fn contains_blob(&self, key: Digest) -> io::Result<bool> {
        if self.staging.contains(key).await {
            return Ok(true);
        }
        Ok(self.db.get(self.entry_key(key)).await.map_err(other)?.is_some())
    }

    /// Fetch bytes stored under `key`, if present.
    async fn get_blob(&self, key: Digest) -> io::Result<Option<Bytes>> {
        if let Some(bytes) = self.staging.get(key).await? {
            return Ok(Some(bytes));
        }
        let Some(entry) = self.get_entry(key).await? else {
            return Ok(None);
        };
        Ok(Some(self.get_range(entry.pack_id, entry.offset, entry.length).await?))
    }

    /// Stages `bytes` under `key` durably in `staging`.
    ///
    /// If enough has accumulated since the last flush, by size or by
    /// time (see `Flushing::is_due`), triggers `flush_pending` in the
    /// background rather than waiting on it, so this call returns as
    /// soon as `bytes` is durable locally. Once `flush_pending` has
    /// failed `MAX_CONSECUTIVE_FLUSH_FAILURES` times in a row, switches
    /// to waiting on it and propagating its error instead, so a
    /// persistently broken flush path is surfaced to callers rather than
    /// growing `staging` without bound.
    async fn put_blob(&self, key: Digest, bytes: Bytes) -> io::Result<()> {
        let active_len = self.staging.put(key, bytes).await?;

        if !self.flushing.is_due(active_len) {
            return Ok(());
        }

        // Group commit can report this same crossed threshold to every
        // `put_blob` call batched into the same write; only the one that
        // wins this claim spawns (or waits on) a flush at all.
        let Some(guard) = self.flushing.try_claim() else {
            return Ok(());
        };

        // The one deliberate exception to flushing staying off the write
        // path: once it's failed this many times in a row, run it inline
        // and propagate its error instead of spawning another background
        // attempt, so a persistently broken flush surfaces to callers
        // rather than growing `staging` unboundedly in silence.
        if self.flushing.failures() >= Self::MAX_CONSECUTIVE_FLUSH_FAILURES {
            return flush_pending(
                self.db,
                self.blobs,
                self.staging,
                self.cas_prefix,
                self.flushing,
                guard,
            )
            .await;
        }

        let db = self.db.clone();
        let blobs = Arc::clone(self.blobs);
        let staging = Arc::clone(self.staging);
        let cas_prefix = self.cas_prefix.to_string();
        let flushing = self.flushing.clone();
        tokio::spawn(async move {
            let _ = flush_pending(&db, &blobs, &staging, &cas_prefix, &flushing, guard).await;
        });
        Ok(())
    }

    /// Consolidates all currently-staged entries into pack objects.
    ///
    /// If another call is already in progress, this returns immediately
    /// without doing anything, rather than waiting its turn.
    pub async fn flush_pending(&self) -> io::Result<()> {
        let Some(guard) = self.flushing.try_claim() else {
            return Ok(());
        };
        flush_pending(self.db, self.blobs, self.staging, self.cas_prefix, self.flushing, guard)
            .await
    }

    /// Fetch and decode chunk(s).
    async fn load(&self, digest: &Digest) -> io::Result<Option<(ContentFlags, Bytes)>> {
        let Some(stored) = self.get_blob(*digest).await? else {
            return Ok(None);
        };
        let dec = self.codec;
        task::spawn_blocking(move || dec.decode(stored).map(Some))
            .await
            .expect("decode should not panic")
    }

    /// Write one chunk(s) under `key`, skipping the encode step entirely
    /// if `key` is already stored.
    async fn save(&self, key: Digest, flags: ContentFlags, bytes: Vec<u8>) -> io::Result<()> {
        if self.contains_blob(key).await? {
            return Ok(());
        }
        let enc = self.codec;

        // Encoding runs in its own `spawn_blocking`, independent of however
        // the backend performs the write itself: it's CPU-bound work that
        // always needs to stay off the async executor.
        let encoded = task::spawn_blocking(move || enc.encode(flags, bytes))
            .await
            .expect("encode should not panic");
        self.put_blob(key, Bytes::from(encoded)).await
    }

    /// Reads the content at `digest`, if present.
    pub async fn get<B: FromBytes>(&self, digest: &Digest) -> io::Result<Option<B>> {
        let mut bytes = Vec::new();
        if !self.read_into(digest, &mut bytes).await? {
            return Ok(None);
        }

        let content = B::from_bytes(Bytes::from(bytes))
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("{e:?}")))?;

        Ok(Some(content))
    }

    /// Store `content`, addressed by its own digest, and return that
    /// digest. A thin wrapper over `copy_from`, over the already
    /// in-memory bytes.
    pub async fn put<B: ToBytes>(&self, content: &B) -> io::Result<Digest> {
        let bytes =
            content.to_bytes().unwrap_or_else(|_| panic!("serializing to bytes should not fail"));
        let len = bytes.len() as u64;
        self.copy_from(len, &mut io::Cursor::new(bytes)).await
    }

    /// Reads the content at `digest` if present and write it to `w`.
    ///
    /// `get` is the better choice for values small enough that this doesn't matter.
    pub async fn read_into<W>(&self, digest: &Digest, w: &mut W) -> io::Result<bool>
    where
        W: AsyncWrite + Unpin,
    {
        let Some((flags, decoded)) = self.load(digest).await? else {
            return Ok(false);
        };

        if !flags.contains(ContentFlags::CHUNKED) {
            if Hasher::new().part(&decoded).digest() != *digest {
                return Err(invalid_data("direct content digest mismatch"));
            }
            w.write_all(&decoded).await?;
            return Ok(true);
        }

        let chunks = decode_chunks(&decoded)?;
        let recomputed = {
            let mut h = Hasher::new();
            h.parts(chunks.iter().map(|c| c.digest.as_ref()));
            h.digest()
        };
        if recomputed != *digest {
            return Err(invalid_data("chunks digest mismatch"));
        }

        // Loads run up to `MAX_CONCURRENT_CHUNKS` ahead of the writer, so
        // fetching one chunk overlaps with writing out the previous one.
        // `buffered` (not `buffer_unordered`) keeps results in the original
        // chunk order, which `w` needs.
        let cas = *self;
        let mut loads = stream::iter(chunks)
            .map(move |chunk| async move {
                let result = cas.load(&chunk.digest).await;
                (chunk, result)
            })
            .buffered(Self::MAX_CONCURRENT_CHUNKS);

        while let Some((chunk, result)) = loads.next().await {
            let Some((chunk_flags, bytes)) = result? else {
                return Err(invalid_data(format!("missing chunk {:x}", chunk.digest)));
            };
            if chunk_flags.contains(ContentFlags::CHUNKED) {
                return Err(invalid_data(format!("nested chunk {:x}", chunk.digest)));
            }
            if bytes.len() as u32 != chunk.len {
                return Err(invalid_data(format!("chunk length mismatch {:x}", chunk.digest)));
            }
            if Hasher::new().part(&bytes).digest() != chunk.digest {
                return Err(invalid_data(format!("chunk digest mismatch {:x}", chunk.digest)));
            }
            w.write_all(&bytes).await?;
        }
        Ok(true)
    }

    /// Store the content read from `r` of `len` bytes, addressed by its own
    /// digest.
    pub async fn copy_from<R>(&self, len: u64, r: &mut R) -> io::Result<Digest>
    where
        R: AsyncRead + Unpin,
    {
        let mut cdc = self.chunking.reader(len, r);
        let mut chunks = pin!(cdc.as_stream());

        let mut chunks_hasher = Hasher::new();
        let mut chunks_bytes = Vec::new();
        let mut last_chunk_digest = None;
        let mut total: u64 = 0;

        // `save`s run up to `MAX_CONCURRENT_CHUNKS` at a time instead of
        // one at a time: chunking itself has to stay sequential (each
        // chunk's boundary depends on a rolling hash over what came
        // before it), but staging a chunk doesn't need to block finding
        // the next one. This is also what lets group commit batch this
        // call's own chunks together, which it otherwise never would --
        // batching only happens across whatever's in flight at once, and
        // a caller that awaits each of its own writes serially never has
        // more than one in flight.
        let mut saves = FuturesUnordered::new();
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk?;
            total += chunk.length as u64;
            let digest = Hasher::new().part(&chunk.data).digest();
            chunks_bytes.put_slice(digest.as_ref());
            chunks_bytes.put_u32(chunk.length as u32);
            chunks_hasher.part(digest.as_ref());
            last_chunk_digest = Some(digest);

            // Keep at most `MAX_CONCURRENT_CHUNKS` in flight: wait for
            // whichever finishes first before adding another, rather
            // than capping how many chunks get read ahead.
            if saves.len() >= Self::MAX_CONCURRENT_CHUNKS {
                saves.next().await.expect("just checked saves is non-empty")?;
            }
            saves.push(self.save(digest, ContentFlags::empty(), chunk.data));
        }
        // Chunking is done, but up to `MAX_CONCURRENT_CHUNKS` saves from
        // the last iterations are still in flight and were never waited
        // on above.
        while let Some(result) = saves.next().await {
            result?;
        }

        if total != len {
            // The reader ended before supplying all of `len` bytes.
            //
            // `Take` silently short-reads on early EOF instead of erroring,
            // so this has to be checked explicitly. The chunks already written
            // above are (harmless) orphans.
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("reader ended {} bytes short of the declared length {len}", len - total),
            ));
        }

        if chunks_bytes.is_empty() {
            // No chunks were emitted. The length check above already
            // guarantees `total == len`, so this can only mean `len` was 0.
            //
            // A blob digest always needs at least one chunk digest to hash
            // over, so treat empty content as exactly one (empty) chunk instead.
            // Falls through to the single-chunk shortcut below.
            let digest = Hasher::new().part([]).digest();
            chunks_bytes.put_slice(digest.as_ref());
            chunks_bytes.put_u32(0);
            last_chunk_digest = Some(digest);
            self.save(digest, ContentFlags::empty(), Vec::new()).await?;
        }

        // Each chunk appends exactly one 36-byte record, so this always holds.
        // This is what makes `len() == 36` below a reliable way to detect
        // "exactly one chunk" without a separate counter.
        debug_assert!(chunks_bytes.len().is_multiple_of(36));

        if chunks_bytes.len() == 36 {
            // Exactly one chunk was emitted, so its own digest is already
            // the blob digest, already written above under that key, and
            // there's nothing left to do. This also means a small blob
            // and the same content appearing as one chunk inside a larger
            // blob dedup against each other.
            Ok(last_chunk_digest.expect("len() == 36 implies last_digest was set"))
        } else {
            let chunks_digest = chunks_hasher.digest();
            self.save(chunks_digest, ContentFlags::CHUNKED, chunks_bytes).await?;
            Ok(chunks_digest)
        }
    }
}

fn entry_key(cas_prefix: &str, key: Digest) -> Vec<u8> {
    let mut buf = Vec::with_capacity(cas_prefix.len() + 7 + 32);
    buf.extend_from_slice(cas_prefix.as_bytes());
    buf.extend_from_slice(b"sha256/");
    buf.extend_from_slice(key.as_ref());
    buf
}

fn pack_path(cas_prefix: &str, pack_id: Digest) -> Path {
    Path::from(cas_prefix).join("sha256").join(format!("{pack_id:x}"))
}

/// Consolidates every currently-pending `staging` segment into pack
/// objects, taking owned references so `put_blob`'s background trigger
/// can run this after the `Cas<'_>` that requested it has gone out of
/// scope.
///
/// `_guard` must come from `Flushing::try_claim`: callers claim it
/// before deciding whether to spawn or wait on this at all, not just
/// before running it, so it's held for this call's whole duration.
async fn flush_pending(
    db: &slatedb::Db,
    blobs: &Arc<dyn ObjectStore>,
    staging: &Staging,
    cas_prefix: &str,
    flushing: &Flushing,
    _guard: OwnedMutexGuard<()>,
) -> io::Result<()> {
    let mut segments = staging.pending_segments();
    if staging.active_len() > 0 {
        segments.push(staging.rotate().await?);
        flushing.reset_staging_since();
    }
    if segments.is_empty() {
        flushing.record_success();
        return Ok(());
    }

    let result = flush_segments(db, blobs, staging, cas_prefix, segments).await;
    match &result {
        Ok(()) => flushing.record_success(),
        Err(_) => flushing.record_failure(),
    }
    result
}

async fn flush_segments(
    db: &slatedb::Db,
    blobs: &Arc<dyn ObjectStore>,
    staging: &Staging,
    cas_prefix: &str,
    segments: Vec<staging::FileId>,
) -> io::Result<()> {
    for segment in segments {
        let staged = staging.entries(segment).await?;
        if staged.is_empty() {
            staging.finish(segment).await?;
            continue;
        }

        // Each staged value is kept as its own `Bytes`, not copied into
        // one contiguous buffer, since `PutPayload` is itself a cheaply
        // cloneable sequence of `Bytes`.
        let pack_id = Hasher::new().parts(staged.iter().map(|(_, b)| b.as_ref())).digest();
        let mut entries = Vec::with_capacity(staged.len());
        let mut chunks = Vec::with_capacity(staged.len());
        let mut offset: u64 = 0;
        for (digest, bytes) in staged {
            let length = bytes.len() as u64;
            entries.push((digest, Entry { pack_id, offset, length }));
            offset += length;
            chunks.push(bytes);
        }

        let path = pack_path(cas_prefix, pack_id);
        blobs.put(&path, PutPayload::from_iter(chunks)).await.map_err(io::Error::from)?;

        let mut batch = slatedb::WriteBatch::new();
        for (digest, entry) in entries {
            batch.put_bytes(Bytes::from(entry_key(cas_prefix, digest)), entry.encode());
        }
        db.write(batch).await.map_err(other)?;

        staging.finish(segment).await?;
    }
    Ok(())
}

/// A reference to one chunk: its digest and its length.
struct Chunk {
    digest: Digest,
    len:    u32,
}

/// Decode chunks into its ordered chunk references.
///
/// The format is a flat sequence of 36-byte records (`digest[32] || len: u32 be`).
fn decode_chunks(bytes: &[u8]) -> io::Result<Vec<Chunk>> {
    if !bytes.len().is_multiple_of(36) {
        return Err(invalid_data("chunks body length is not a multiple of 36"));
    }
    let mut chunks = Vec::with_capacity(bytes.len() / 36);
    let mut buf = bytes;
    let mut digest = [0u8; 32];
    while buf.has_remaining() {
        buf.copy_to_slice(&mut digest);
        chunks.push(Chunk { digest: Digest::new(digest), len: buf.get_u32() });
    }
    Ok(chunks)
}
