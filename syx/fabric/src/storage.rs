//! Content-addressed blob storage: chunking, encoding on the way in,
//! decoding and verifying on the way out.
//!
//! # Data layout
//!
//! ## Staging
//!
//! Not-yet-packed blobs are staged in `bitcask`, a local durable log, not
//! in `db`. `db` only ever holds pointers to already-packed content; see
//! `bitcask`'s own module doc for why.
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
//!   when `flush_pending` consolidates a bitcask segment into a pack.
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
use tokio::task;

mod bitcask;

#[cfg(test)]
mod tests;

pub(crate) use bitcask::Bitcask;

fn invalid_data(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

fn other(e: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::other(e)
}

/// The default `cas_prefix`, for the common case of `store` existing
/// solely for this `Graph`'s own blob storage.
pub(crate) const DEFAULT_CAS_PREFIX: &str = "cas/";

/// The default `packs_threshold`, 32 MiB, enough to consolidate several
/// dozen chunks per pack.
pub(crate) const DEFAULT_PACKS_THRESHOLD: u64 = Chunking::AVG_SIZE as u64 * 64;

/// The default `max_staging_duration`: bounds how long a blob can stay
/// invisible to every other reader of `Graph` even when write volume
/// never crosses `packs_threshold` on its own.
pub(crate) const DEFAULT_MAX_STAGING_DURATION: Duration = Duration::from_secs(30);

/// Flush behavior: when to consolidate staged entries into a pack, and
/// bookkeeping for that process. A plain data container; the operations
/// that use it live on `Cas`.
#[derive(Clone)]
pub(crate) struct Flushing {
    /// Serializes `flush_pending`.
    mutex: Arc<tokio::sync::Mutex<()>>,
    /// Consecutive `flush_pending` failures, reset to 0 on success.
    /// `put_blob` reads this to decide whether to run the next flush in
    /// the background or wait on it and propagate its error.
    failures: Arc<AtomicU32>,
    /// How many bytes to stage before consolidating into a pack.
    threshold: u64,
    /// How long to let a blob sit staged, unpacked, before consolidating
    /// regardless of `threshold`.
    max_staging_duration: Duration,
    /// When the active segment currently being staged into started.
    staging_since: Arc<std::sync::Mutex<Instant>>,
}

impl Flushing {
    /// Builds `Flushing` for `Graph` to hold directly. `Graph::Builder`
    /// is the configuration surface, resolving defaults and overrides;
    /// this just builds what it resolves.
    pub(crate) fn new(threshold: u64, max_staging_duration: Duration) -> Self {
        Self {
            mutex: Arc::new(tokio::sync::Mutex::new(())),
            failures: Arc::new(AtomicU32::new(0)),
            threshold,
            max_staging_duration,
            staging_since: Arc::new(std::sync::Mutex::new(Instant::now())),
        }
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
    store:      &'a Arc<dyn ObjectStore>,
    bitcask:    &'a Arc<Bitcask>,
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

    /// Only `Graph::cas()` calls this.
    pub(crate) fn new(
        db: &'a slatedb::Db,
        store: &'a Arc<dyn ObjectStore>,
        bitcask: &'a Arc<Bitcask>,
        cas_prefix: &'a str,
        flushing: &'a Flushing,
        chunking: Chunking,
        codec: Codec,
    ) -> Self {
        Self { db, store, bitcask, cas_prefix, flushing, chunking, codec }
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
            self.store.get_opts(&self.pack_path(pack_id), opts).await.map_err(io::Error::from)?;
        Ok(result.bytes().await?)
    }

    /// Whether `key` is already stored, without fetching its value.
    async fn contains_blob(&self, key: Digest) -> io::Result<bool> {
        if self.bitcask.contains(key).await {
            return Ok(true);
        }
        Ok(self.db.get(self.entry_key(key)).await.map_err(other)?.is_some())
    }

    /// Fetch bytes stored under `key`, if present.
    async fn get_blob(&self, key: Digest) -> io::Result<Option<Bytes>> {
        if let Some(bytes) = self.bitcask.get(key).await? {
            return Ok(Some(bytes));
        }
        let Some(entry) = self.get_entry(key).await? else {
            return Ok(None);
        };
        Ok(Some(self.get_range(entry.pack_id, entry.offset, entry.length).await?))
    }

    /// Stages `bytes` under `key` durably in `bitcask`.
    ///
    /// If enough has accumulated since the last flush, by size
    /// (`flushing.threshold`) or by time (`flushing.max_staging_duration`),
    /// triggers `flush_pending` in the background rather than waiting on
    /// it, so this call returns as soon as `bytes` is durable locally.
    /// Once `flush_pending` has failed `MAX_CONSECUTIVE_FLUSH_FAILURES`
    /// times in a row, switches to waiting on it and propagating its
    /// error instead, so a persistently broken flush path is surfaced to
    /// callers rather than growing `bitcask` without bound.
    async fn put_blob(&self, key: Digest, bytes: Bytes) -> io::Result<()> {
        self.bitcask.put(key, bytes).await?;

        let due = self.bitcask.active_len() >= self.flushing.threshold
            || self.staging_elapsed() >= self.flushing.max_staging_duration;
        if !due {
            return Ok(());
        }

        if self.flushing.failures.load(Ordering::Relaxed) >= Self::MAX_CONSECUTIVE_FLUSH_FAILURES {
            return self.flush_pending().await;
        }

        let db = self.db.clone();
        let store = Arc::clone(self.store);
        let bitcask = Arc::clone(self.bitcask);
        let cas_prefix = self.cas_prefix.to_string();
        let flushing = self.flushing.clone();
        tokio::spawn(async move {
            let _ = flush_pending(&db, &store, &bitcask, &cas_prefix, &flushing).await;
        });
        Ok(())
    }

    fn staging_elapsed(&self) -> Duration {
        self.flushing.staging_since.lock().unwrap().elapsed()
    }

    /// Consolidates all currently-staged entries into pack objects.
    ///
    /// If another call is already in progress, this returns immediately
    /// without doing anything, rather than waiting its turn.
    pub async fn flush_pending(&self) -> io::Result<()> {
        flush_pending(self.db, self.store, self.bitcask, self.cas_prefix, self.flushing).await
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

        for chunk in chunks {
            let Some((chunk_flags, bytes)) = self.load(&chunk.digest).await? else {
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
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk?;
            total += chunk.length as u64;
            let digest = Hasher::new().part(&chunk.data).digest();
            chunks_bytes.put_slice(digest.as_ref());
            chunks_bytes.put_u32(chunk.length as u32);
            chunks_hasher.part(digest.as_ref());
            last_chunk_digest = Some(digest);
            self.save(digest, ContentFlags::empty(), chunk.data).await?;
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

/// Consolidates every currently-pending `bitcask` segment into pack
/// objects, taking owned references so `put_blob`'s background trigger
/// can run this after the `Cas<'_>` that requested it has gone out of
/// scope.
///
/// If another call is already in progress, this returns immediately
/// without doing anything, rather than waiting its turn.
async fn flush_pending(
    db: &slatedb::Db,
    store: &Arc<dyn ObjectStore>,
    bitcask: &Bitcask,
    cas_prefix: &str,
    flushing: &Flushing,
) -> io::Result<()> {
    let Ok(_guard) = flushing.mutex.try_lock() else {
        return Ok(());
    };

    let mut segments = bitcask.pending_segments().await;
    if bitcask.active_len() > 0 {
        segments.push(bitcask.rotate().await?);
        *flushing.staging_since.lock().unwrap() = Instant::now();
    }
    if segments.is_empty() {
        flushing.failures.store(0, Ordering::Relaxed);
        return Ok(());
    }

    let result = flush_segments(db, store, bitcask, cas_prefix, segments).await;
    match &result {
        Ok(()) => flushing.failures.store(0, Ordering::Relaxed),
        Err(_) => {
            flushing.failures.fetch_add(1, Ordering::Relaxed);
        }
    }
    result
}

async fn flush_segments(
    db: &slatedb::Db,
    store: &Arc<dyn ObjectStore>,
    bitcask: &Bitcask,
    cas_prefix: &str,
    segments: Vec<bitcask::Segment>,
) -> io::Result<()> {
    for segment in segments {
        let staged = bitcask.entries(segment).await?;
        if staged.is_empty() {
            bitcask.finish(segment).await?;
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
        store.put(&path, PutPayload::from_iter(chunks)).await.map_err(io::Error::from)?;

        let mut batch = slatedb::WriteBatch::new();
        for (digest, entry) in entries {
            batch.put_bytes(Bytes::from(entry_key(cas_prefix, digest)), entry.encode());
        }
        db.write(batch).await.map_err(other)?;

        bitcask.finish(segment).await?;
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
