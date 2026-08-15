//! Content-addressed blob storage: chunking, encoding on the way in,
//! decoding and verifying on the way out.
//!
//! # Data layout
//!
//! ## Packing
//!
//! One object per chunk means one backend API call per chunk, which
//! gets expensive as blob count grows. Consolidating many small objects
//! into fewer, larger packs cuts that down.
//!
//! ## Staging
//!
//! With `cas_prefix` as `"cas/"`:
//!
//! - `cas/pending_bytes`: `u64` big-endian, how many `Inline` bytes are currently staged.
//! - `cas/pending_keys`: a flat concatenation of 32-byte digests, still staged.
//! - `cas/sha256/{digest}`: one entry per blob/chunk, keyed by its own 32-byte digest (raw bytes,
//!   not hex; `sha256` names the hashing scheme so a future switch to a different one, e.g.
//!   `blake3`, can live alongside these keys instead of colliding with them). The value is
//!   [`Entry::encode`]'s output, either `Inline` or `Packed` below.
//!
//! `Inline`: `[payload][ContentFlags][EntryFlags]`
//! The content exactly as [`content_addressing::Codec::encode`] produced it
//! (`[payload][ContentFlags]`), plus one more trailing tag byte recording that this value *is*
//! the content, not a pointer to it.
//!
//! `Packed`: `[pack_id: 32 bytes][offset: u64][length: u64][EntryFlags]`
//! This entry has moved out of `db` and into a pack object. `pack_id` names which one
//! (`cas/sha256/{pack_id:x}`, see the pack layout below); `offset`/`length` say where within it.
//! Written once, when `flush_pending` consolidates staged entries into a pack and flips each one
//! from `Inline` to `Packed` in the same `WriteBatch` that makes the flip durable.
//!
//! ## Object Store
//!
//! - `cas/sha256/{pack_id:x}`: one object per `flush_pending` run, hex-encoded. A pack object's
//!   bytes are just its packed entries' concatenated bytes, each one dropping its trailing
//!   `EntryFlags` byte first. A `Packed` entry's `offset`/`length` say where its bytes start and
//!   how long they run within that concatenation.
use std::io;
use std::ops::Range;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{
    AtomicU32,
    Ordering,
};

use bytes::{
    Buf,
    BufMut,
    Bytes,
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

mod entry;

#[cfg(test)]
mod tests;

fn invalid_data(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

fn other(e: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::other(e)
}

/// The merge operator `db` must be opened with for `Cas` to work
/// correctly against it. `Cas`/`Graph` never open `db` themselves;
/// `Graph::Builder` registers this instead.
pub(crate) fn merge_operator() -> Box<dyn slatedb::MergeOperator + Send + Sync> {
    entry::merge_operator()
}

/// The default `cas_prefix`, for the common case of `store` existing
/// solely for this `Graph`'s own blob storage.
pub(crate) const DEFAULT_CAS_PREFIX: &str = "cas/";

/// The default `packs_threshold`, 32 MiB, enough to consolidate several
/// dozen chunks per pack.
pub(crate) const DEFAULT_PACKS_THRESHOLD: u64 = Chunking::AVG_SIZE as u64 * 64;

/// Flush behavior: when to consolidate staged entries into a pack, and
/// bookkeeping for that process. A plain data container; the operations
/// that use it live on `Cas`.
#[derive(Clone)]
pub(crate) struct Flushing {
    /// Serializes `flush_pending`.
    mutex:     Arc<tokio::sync::Mutex<()>>,
    /// Consecutive `flush_pending` failures, reset to 0 on success.
    /// `put_blob`'s opportunistic call reads this to decide whether to
    /// swallow an error or propagate it.
    failures:  Arc<AtomicU32>,
    /// How many bytes to stage before consolidating into a pack.
    threshold: u64,
}

impl Flushing {
    /// Builds `Flushing` for `Graph` to hold directly. `Graph::Builder`
    /// is the configuration surface, resolving defaults and overrides;
    /// this just builds what it resolves.
    pub(crate) fn new(threshold: u64) -> Self {
        Self {
            mutex: Arc::new(tokio::sync::Mutex::new(())),
            failures: Arc::new(AtomicU32::new(0)),
            threshold,
        }
    }
}

/// Where an entry currently lives.
enum Entry {
    /// Still staged: the raw bytes themselves, opaque here but really
    /// `[payload][ContentFlags]`, as `Codec::encode` produced it.
    Inline(Bytes),
    /// Migrated: where to find it in an already-durable pack.
    Packed { pack_id: Digest, offset: u64, length: u64 },
}

/// The blob-storage facet of a `Graph`: chunking, encoding/decoding, and
/// physical storage of blobs, addressed by digest.
/// A borrowed view, not an owned type. Construct one fresh per call via
/// `Graph::cas()` rather than holding onto one.
#[derive(Clone, Copy)]
pub struct Cas<'a> {
    db:         &'a slatedb::Db,
    store:      &'a Arc<dyn ObjectStore>,
    cas_prefix: &'a str,
    flushing:   &'a Flushing,
    chunking:   Chunking,
    codec:      Codec,
}

impl<'a> Cas<'a> {
    /// How many consecutive `flush_pending` failures `put_blob` tolerates.
    const MAX_CONSECUTIVE_FLUSH_FAILURES: u32 = 3;

    /// Only `Graph::cas()` calls this.
    pub(crate) fn new(
        db: &'a slatedb::Db,
        store: &'a Arc<dyn ObjectStore>,
        cas_prefix: &'a str,
        flushing: &'a Flushing,
        chunking: Chunking,
        codec: Codec,
    ) -> Self {
        Self { db, store, cas_prefix, flushing, chunking, codec }
    }

    fn entry_key(&self, key: Digest) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.cas_prefix.len() + 7 + 32);
        buf.extend_from_slice(self.cas_prefix.as_bytes());
        buf.extend_from_slice(b"sha256/");
        buf.extend_from_slice(key.as_ref());
        buf
    }

    fn pending_bytes_key(&self) -> Vec<u8> {
        format!("{}pending_bytes", self.cas_prefix).into_bytes()
    }

    fn pending_keys_key(&self) -> Vec<u8> {
        format!("{}pending_keys", self.cas_prefix).into_bytes()
    }

    /// Test-only: `staged` finds pending entries via `pending_keys_key`,
    /// not by scanning this range.
    #[cfg(test)]
    fn entry_prefix(&self) -> Vec<u8> {
        format!("{}sha256/", self.cas_prefix).into_bytes()
    }

    /// How many distinct keys have ever been staged or packed
    /// under this store's prefix.
    #[cfg(test)]
    pub(crate) async fn entry_count(&self) -> io::Result<usize> {
        let mut iter = self.db.scan_prefix(self.entry_prefix(), ..).await.map_err(other)?;
        let mut n = 0;
        while iter.next().await.map_err(other)?.is_some() {
            n += 1;
        }
        Ok(n)
    }

    async fn pending_bytes(&self) -> io::Result<u64> {
        match self.db.get(self.pending_bytes_key()).await.map_err(other)? {
            Some(bytes) => Ok(u64::from_be_bytes(bytes.as_ref().try_into().unwrap_or_default())),
            None => Ok(0),
        }
    }

    /// Digests currently staged (not yet packed), in the order they
    /// were merged into `pending_keys_key`.
    async fn pending_keys(&self) -> io::Result<Vec<Digest>> {
        let Some(bytes) = self.db.get(self.pending_keys_key()).await.map_err(other)? else {
            return Ok(Vec::new());
        };
        if !bytes.len().is_multiple_of(32) {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "malformed pending key list"));
        }
        Ok(bytes.chunks_exact(32).map(|c| Digest::new(c.try_into().unwrap())).collect())
    }

    /// Fetch and decode the entry stored under `key`, if present.
    async fn get_entry(&self, key: Digest) -> io::Result<Option<Entry>> {
        let Some(raw) = self.db.get(self.entry_key(key)).await.map_err(other)? else {
            return Ok(None);
        };
        Entry::decode(&raw).map(Some)
    }

    /// Currently-staged entries, with their still-`Inline` bytes, found
    /// via `pending_keys_key` rather than by scanning every entry ever
    /// held, packed or not.
    async fn staged(&self) -> io::Result<Vec<(Digest, Bytes)>> {
        let mut staged = Vec::new();
        for digest in self.pending_keys().await? {
            let Some(entry) = self.get_entry(digest).await? else {
                // This is purely internal bookkeeping: `put_blob` always
                // writes the entry before merging its digest into
                // pending_keys, and entries are never deleted, so
                // reaching here would mean that invariant itself broke.
                // Still safe to skip. Nothing to do for a missing entry.
                continue;
            };
            let Entry::Inline(bytes) = entry else {
                // `flushing.mutex` ensures only one `flush_pending` call
                // runs at a time, and only `commit_packed` flips Inline
                // to Packed, so reaching here would mean that
                // serialization itself broke. Still safe to skip.
                // Nothing to do for an already packed entry.
                continue;
            };
            staged.push((digest, bytes));
        }
        Ok(staged)
    }

    /// Atomically flips `entries` to `Packed` and resets the pending
    /// counters. Called once their bytes are durable in a pack object.
    async fn commit_packed(&self, entries: Vec<(Digest, Entry)>) -> io::Result<()> {
        let mut batch = slatedb::WriteBatch::new();
        for (digest, entry) in entries {
            batch.put_bytes(Bytes::from(self.entry_key(digest)), entry.encode());
        }
        batch.put_bytes(
            Bytes::from(self.pending_bytes_key()),
            Bytes::copy_from_slice(&0u64.to_be_bytes()),
        );
        batch.put_bytes(Bytes::from(self.pending_keys_key()), Bytes::new());
        self.db.write(batch).await.map_err(other)?;
        Ok(())
    }

    fn pack_path(&self, pack_id: Digest) -> Path {
        Path::from(self.cas_prefix).join("sha256").join(format!("{pack_id:x}"))
    }

    /// Fetch `length` bytes at `offset` from pack `pack_id`.
    async fn get_range(&self, pack_id: Digest, offset: u64, length: u64) -> io::Result<Bytes> {
        let range: Range<u64> = offset..offset + length;
        let opts = GetOptions { range: Some(range.into()), ..Default::default() };
        let result =
            self.store.get_opts(&self.pack_path(pack_id), opts).await.map_err(io::Error::from)?;
        Ok(result.bytes().await?)
    }

    /// Write `payload` as one new pack object identified by `pack_id`.
    async fn write_pack(&self, pack_id: Digest, payload: PutPayload) -> io::Result<()> {
        self.store.put(&self.pack_path(pack_id), payload).await.map_err(io::Error::from)?;
        Ok(())
    }

    /// Whether `key` is already stored, without fetching its value.
    async fn contains_blob(&self, key: Digest) -> io::Result<bool> {
        Ok(self.db.get(self.entry_key(key)).await.map_err(other)?.is_some())
    }

    /// Fetch bytes stored under `key`, if present.
    async fn get_blob(&self, key: Digest) -> io::Result<Option<Bytes>> {
        let Some(entry) = self.get_entry(key).await? else {
            return Ok(None);
        };
        match entry {
            Entry::Inline(bytes) => Ok(Some(bytes)),
            Entry::Packed { pack_id, offset, length } => {
                Ok(Some(self.get_range(pack_id, offset, length).await?))
            }
        }
    }

    /// Store `bytes` under `key`.
    ///
    /// If enough is already staged to cross `flushing.threshold`,
    /// flushes first before staging `bytes` itself.
    ///
    /// Tolerates up to `MAX_CONSECUTIVE_FLUSH_FAILURES` consecutive
    /// flush failures this way; past that, propagates the error
    /// instead of continuing to accept writes that would never get packed.
    async fn put_blob(&self, key: Digest, bytes: Bytes) -> io::Result<()> {
        if self.pending_bytes().await? >= self.flushing.threshold
            && let Err(e) = self.flush_pending().await
        {
            let failures = self.flushing.failures.load(Ordering::Relaxed);
            if failures >= Self::MAX_CONSECUTIVE_FLUSH_FAILURES {
                return Err(e);
            }
        }
        let len = bytes.len() as u64;
        let entry = Entry::Inline(bytes);
        let mut batch = slatedb::WriteBatch::new();
        batch.put_bytes(Bytes::from(self.entry_key(key)), entry.encode());
        batch.merge(self.pending_bytes_key(), len.to_be_bytes());
        batch.merge(self.pending_keys_key(), key.as_ref());
        self.db.write(batch).await.map_err(other)?;
        Ok(())
    }

    /// Consolidates all currently-staged entries into one new pack object.
    ///
    /// If another call is already in progress, this returns immediately
    /// without doing anything, rather than waiting its turn.
    pub async fn flush_pending(&self) -> io::Result<()> {
        let Ok(_guard) = self.flushing.mutex.try_lock() else {
            return Ok(());
        };

        let staged = self.staged().await?;
        if staged.is_empty() {
            self.flushing.failures.store(0, Ordering::Relaxed);
            return Ok(());
        }

        // Each `Inline` value is kept as its own `Bytes`, not copied into
        // one contiguous buffer, since `PutPayload` is itself a cheaply
        // cloneable sequence of `Bytes`, so `write_pack` below takes
        // them as-is.
        let pack_id = Hasher::new().parts(staged.iter().map(|(_, b)| b.as_ref())).digest();
        let mut entries = Vec::with_capacity(staged.len());
        let mut chunks = Vec::with_capacity(staged.len());
        let mut offset: u64 = 0;
        for (digest, bytes) in staged {
            let length = bytes.len() as u64;
            entries.push((digest, Entry::Packed { pack_id, offset, length }));
            offset += length;
            chunks.push(bytes);
        }

        let result = match self.write_pack(pack_id, PutPayload::from_iter(chunks)).await {
            Ok(()) => self.commit_packed(entries).await,
            Err(e) => Err(e),
        };
        match &result {
            Ok(()) => self.flushing.failures.store(0, Ordering::Relaxed),
            Err(_) => {
                self.flushing.failures.fetch_add(1, Ordering::Relaxed);
            }
        }
        result
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
