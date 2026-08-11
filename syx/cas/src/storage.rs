//! Content-addressed blob storage: chunking, encoding on the way in,
//! decoding and verifying on the way out. Blobs are staged in `slatedb`
//! before being consolidated into pack objects in a wrapped `ObjectStore`.
//!
//! # Why pack at all
//!
//! One object per chunk means one backend API call per chunk, which
//! gets expensive (S3 request pricing, rate limits) as blob count
//! grows. Consolidating many small objects into fewer, larger packs
//! cuts that down, the same way `git`/`restic` pack loose objects.
//!
//! # Why stage in `slatedb` rather than pack directly
//!
//! Two simpler alternatives were considered and rejected:
//!
//! - Write each chunk as its own object immediately, then repack later: still pays one API call per
//!   chunk up front, plus a GET, a PUT, and a DELETE per chunk to consolidate afterward -- strictly
//!   more calls, not fewer.
//!
//! - Buffer writes in memory and flush a pack once enough accumulates: a crash before the flush
//!   silently loses writes that already returned `Ok` to the caller.
//!
//! Staging in `slatedb` first avoids both: no per-chunk backend object
//! is ever created, and nothing is acknowledged before it's durable.
//! Once enough accumulates, concatenates the staged bytes into one pack
//! object and flips each entry to `Entry::Packed`.
//!
//! # Data layout
//!
//! Two separate namespaces share the name "prefix": `db_prefix`, an `object_store::path::Path`
//! passed straight through to `slatedb::Db::builder` and never touched again here (everything
//! under it -- manifest, WAL, SST files -- is `slatedb`'s own concern), and `prefix`
//! (`Stage`/`Packs`'s own `prefix` field, default `cas/`, see [`StorageBuilder::DEFAULT_PREFIX`]),
//! covered below. Both exist so `db` and/or `packs` can be shared with something else that carves
//! out its own namespace without colliding -- see `StorageBuilder::build`'s collision check.
//!
//! ## `Stage` layout
//!
//! With `prefix` as `"cas/"`:
//!
//! - `cas/pending_bytes`: `u64` big-endian, how many `Inline` bytes are currently staged.
//! - `cas/pending_keys`: a flat concatenation of 32-byte digests, still staged.
//! - `cas/sha256/{digest}`: one entry per blob/chunk, keyed by its own 32-byte digest (raw bytes,
//!   not hex; `sha256` names the hashing scheme so a future switch to a different one, e.g.
//!   `blake3`, can live alongside these keys instead of colliding with them). The value is
//!   [`Entry::encode`]'s output, either `Inline` or `Packed` below.
//!
//! `Inline`: `[payload][ContentFlags][EntryFlags]`
//! The content exactly as [`Encoding::encode`] produced it (`[payload][ContentFlags]`), plus one
//! more trailing tag byte recording that this value *is* the content, not a pointer to it.
//!
//! `Packed`: `[pack_id: 32 bytes][offset: u64][length: u64][EntryFlags]`
//! This entry has moved out of `db` and into a pack object. `pack_id` names which one
//! (`cas/sha256/{pack_id:x}`, see `Packs` layout below); `offset`/`length` say where within it.
//! Written once, when `flush_pending` consolidates staged entries into a pack and flips each one
//! from `Inline` to `Packed` in the same `WriteBatch` that makes the flip durable.
//!
//! ## `Packs` layout
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
    BytesMut,
};
use fastcdc::v2020;
use futures::StreamExt as _;
use object_store::path::Path;
use object_store::{
    GetOptions,
    ObjectStore,
    ObjectStoreExt as _,
    PutPayload,
};
use slatedb::{
    MergeOperator,
    MergeOperatorError,
    WriteBatch,
};
use tokio::io::{
    AsyncRead,
    AsyncReadExt as _,
    AsyncWrite,
    AsyncWriteExt as _,
};
use tokio::task;

use crate::hash::{
    Digest,
    FromBytes,
    Hasher,
    ToBytes,
};
use crate::{
    Chunk,
    Chunking,
    ContentFlags,
    Decoding,
    Encoding,
    Entry,
    EntryFlags,
    Packs,
    Stage,
    Storage,
    StorageBuilder,
    invalid_data,
    other,
};

#[cfg(test)]
#[path = "storage_test.rs"]
mod tests;

impl StorageBuilder {
    /// The default `prefix`, for the common case of `db` and `packs`
    /// existing solely for this `Storage`'s own sake.
    const DEFAULT_PREFIX: &str = "cas/";

    /// The default `packs_threshold`: 32 MiB -- enough to consolidate
    /// several dozen chunks per pack.
    const DEFAULT_PACKS_THRESHOLD: u64 = Chunking::AVG_SIZE as u64 * 64;

    /// The key prefix blobs are staged and packed under. Only needed
    /// when `db` (and/or `packs`) is shared with something else that
    /// needs its own namespace.
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    /// Writes pack objects to `packs` instead of `db`'s own backend
    /// (the default -- see [`Storage::builder`]). Only needed when
    /// content should live somewhere other than wherever `db` persists
    /// itself.
    pub fn packs(mut self, packs: Arc<dyn ObjectStore>) -> Self {
        self.packs_backend = Some(packs);
        self
    }

    /// How many bytes to stage before consolidating into a pack
    /// (defaults to [`StorageBuilder::DEFAULT_PACKS_THRESHOLD`]).
    pub fn packs_threshold(mut self, packs_threshold: u64) -> Self {
        self.packs_threshold = packs_threshold;
        self
    }

    /// Overrides chunking behavior (defaults to [`Chunking::new`]). See
    /// `Chunking`'s own doc -- not safe to change carelessly.
    pub fn chunking(mut self, chunking: Chunking) -> Self {
        self.chunking = chunking;
        self
    }

    /// Overrides encoding behavior (defaults to [`Encoding::new`]). Safe
    /// to change at any time -- see `Encoding`'s own doc.
    pub fn encoding(mut self, encoding: Encoding) -> Self {
        self.encoding = encoding;
        self
    }

    /// Fails if `packs` was never set and `db_prefix`/`prefix` collide.
    pub async fn build(self) -> io::Result<Storage> {
        if self.packs_backend.is_none()
            && Path::from(self.db_prefix.as_str()) == Path::from(self.prefix.as_str())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "db_prefix and prefix must differ to avoid key collisions when \
                    packs defaults to sharing db's own backend: both are {:?}",
                    self.db_prefix
                ),
            ));
        }
        let packs_store = self.packs_backend.unwrap_or_else(|| self.db_backend.clone());
        // TODO: `db` is always opened with only `Stage::merge_operator()`.
        // No way yet for a caller to supply/combine an additional merge
        // operator for another component sharing this same `db`.
        let db = slatedb::Db::builder(self.db_prefix, self.db_backend)
            .with_merge_operator(Stage::merge_operator())
            .build()
            .await
            .map_err(other)?;
        Ok(Storage {
            stage:    Stage {
                db,
                prefix: self.prefix.clone(),
                flushing: Arc::new(tokio::sync::Mutex::new(())),
                flush_failures: Arc::new(AtomicU32::new(0)),
            },
            packs:    Packs {
                store:     packs_store,
                prefix:    self.prefix,
                threshold: self.packs_threshold,
            },
            chunking: self.chunking,
            encoding: self.encoding,
            decoding: Decoding::new(),
        })
    }
}

impl Storage {
    /// How many consecutive `flush_pending` failures `put_blob` tolerates.
    const MAX_CONSECUTIVE_FLUSH_FAILURES: u32 = 3;

    /// Starts building a `Storage`.
    pub fn builder(
        db_prefix: impl Into<String>,
        db_backend: Arc<dyn ObjectStore>,
    ) -> StorageBuilder {
        StorageBuilder {
            db_prefix: db_prefix.into(),
            db_backend,
            packs_backend: None,
            prefix: StorageBuilder::DEFAULT_PREFIX.to_string(),
            packs_threshold: StorageBuilder::DEFAULT_PACKS_THRESHOLD,
            chunking: Chunking::new(),
            encoding: Encoding::new(),
        }
    }

    /// Whether `key` is already stored, without fetching its value.
    async fn contains_blob(&self, key: Digest) -> io::Result<bool> {
        self.stage.contains(key).await
    }

    /// Fetch bytes stored under `key`, if present.
    async fn get_blob(&self, key: Digest) -> io::Result<Option<Bytes>> {
        let Some(entry) = self.stage.get(key).await? else {
            return Ok(None);
        };
        match entry {
            Entry::Inline(bytes) => Ok(Some(bytes)),
            Entry::Packed { pack_id, offset, length } => {
                Ok(Some(self.packs.get_range(pack_id, offset, length).await?))
            }
        }
    }

    /// Store `bytes` under `key`.
    ///
    /// If enough is already staged to cross `packs.threshold`,
    /// flushes first before staging `bytes` itself.
    ///
    /// Tolerates up to `MAX_CONSECUTIVE_FLUSH_FAILURES` consecutive
    /// flush failures this way; past that, propagates the error
    /// instead of continuing to accept writes that would never get packed.
    async fn put_blob(&self, key: Digest, bytes: Bytes) -> io::Result<()> {
        if self.stage.pending_bytes().await? >= self.packs.threshold
            && let Err(e) = self.flush_pending().await
        {
            let failures = self.stage.flush_failures.load(Ordering::Relaxed);
            if failures >= Self::MAX_CONSECUTIVE_FLUSH_FAILURES {
                return Err(e);
            }
        }
        self.stage.put(key, bytes).await
    }

    /// How many distinct keys have ever been staged or packed
    /// under this store's prefix. Test-only.
    #[cfg(test)]
    pub(crate) async fn entry_count(&self) -> io::Result<usize> {
        self.stage.entry_count().await
    }

    /// Consolidates all currently-staged entries into one new pack object.
    ///
    /// If another call is already in progress, this returns immediately
    /// without doing anything, rather than waiting its turn.
    pub async fn flush_pending(&self) -> io::Result<()> {
        let Ok(_guard) = self.stage.flushing.try_lock() else {
            return Ok(());
        };

        let staged = self.stage.staged().await?;
        if staged.is_empty() {
            self.stage.flush_failures.store(0, Ordering::Relaxed);
            return Ok(());
        }

        // Each `Inline` value is kept as its own `Bytes`, not copied into
        // one contiguous buffer -- `PutPayload` is a cheaply cloneable
        // sequence of `Bytes` (`Arc<[Bytes]>`), so `packs.write` below
        // takes them as-is.
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

        let result = match self.packs.write(pack_id, PutPayload::from_iter(chunks)).await {
            Ok(()) => self.stage.commit_packed(entries).await,
            Err(e) => Err(e),
        };
        match &result {
            Ok(()) => self.stage.flush_failures.store(0, Ordering::Relaxed),
            Err(_) => {
                self.stage.flush_failures.fetch_add(1, Ordering::Relaxed);
            }
        }
        result
    }

    /// Fetch and decode chunk(s).
    async fn load(&self, digest: &Digest) -> io::Result<Option<(ContentFlags, Bytes)>> {
        let Some(stored) = self.get_blob(*digest).await? else {
            return Ok(None);
        };
        let dec = self.decoding;
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
        let enc = self.encoding;

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
        // `r` may be a multiplexed/persistent stream where EOF doesn't mark
        // this blob's end, so bound the chunker to exactly `len` bytes
        // rather than reading until EOF.
        let source = r.take(len);
        let mut cdc = v2020::AsyncStreamCDC::new(
            source,
            self.chunking.min_size,
            self.chunking.avg_size,
            self.chunking.max_size,
        );
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
            // the blob digest -- already written above under that key,
            // so there's nothing left to do. This also means a small blob
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

impl Entry {
    /// The tag byte trails the payload rather than leading it, so the
    /// common `Inline` case can append in place -- via `try_into_mut`,
    /// reusing `bytes`' own allocation -- instead of copying a chunk's
    /// entire content (up to a few MiB) just to prepend one byte.
    fn encode(self) -> Bytes {
        match self {
            Entry::Inline(bytes) => {
                let mut buf =
                    bytes.try_into_mut().unwrap_or_else(|bytes| BytesMut::from(&bytes[..]));
                buf.put_u8(EntryFlags::empty().bits());
                buf.freeze()
            }
            Entry::Packed { pack_id, offset, length } => {
                let mut buf = BytesMut::with_capacity(32 + 8 + 8 + 1);
                buf.extend_from_slice(pack_id.as_ref());
                buf.extend_from_slice(&offset.to_be_bytes());
                buf.extend_from_slice(&length.to_be_bytes());
                buf.put_u8(EntryFlags::PACKED.bits());
                buf.freeze()
            }
        }
    }

    fn decode(bytes: &Bytes) -> io::Result<Self> {
        let Some(&tag) = bytes.last() else {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "malformed Entry"));
        };
        let tag = EntryFlags::from_bits_retain(tag);
        if !tag.contains(EntryFlags::PACKED) {
            return Ok(Entry::Inline(bytes.slice(..bytes.len() - 1)));
        }
        if bytes.len() != 32 + 8 + 8 + 1 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "malformed Entry"));
        }
        let mut pack_id = [0u8; 32];
        pack_id.copy_from_slice(&bytes[0..32]);
        let offset = u64::from_be_bytes(bytes[32..40].try_into().unwrap());
        let length = u64::from_be_bytes(bytes[40..48].try_into().unwrap());
        Ok(Entry::Packed { pack_id: Digest::new(pack_id), offset, length })
    }
}

/// `Stage`'s merge operator.
// TODO: assumes it's the only `MergeOperator` registered on `db`. If `db`
// ever gets shared with another user that needs its own merge behavior,
// this needs to compose with that instead of being the sole dispatcher.
struct StorageMergeOperator;

impl MergeOperator for StorageMergeOperator {
    fn merge(
        &self,
        key: &Bytes,
        existing: Option<Bytes>,
        op: Bytes,
    ) -> Result<Bytes, MergeOperatorError> {
        if key.ends_with(b"pending_bytes") {
            let existing = existing
                .map(|v| u64::from_be_bytes(v.as_ref().try_into().unwrap_or_default()))
                .unwrap_or(0);
            let delta = u64::from_be_bytes(op.as_ref().try_into().unwrap_or_default());
            return Ok(Bytes::copy_from_slice(&existing.saturating_add(delta).to_be_bytes()));
        }

        match existing {
            Some(existing) => {
                // Grow `existing` in place when uniquely owned.
                let mut buf = existing.try_into_mut().unwrap_or_else(|b| BytesMut::from(&b[..]));
                buf.extend_from_slice(&op);
                Ok(buf.freeze())
            }
            None => Ok(op),
        }
    }
}

impl Stage {
    /// The merge operator `db` must be opened with for `pending_bytes`
    /// and `pending_keys` to work.
    fn merge_operator() -> Arc<dyn MergeOperator + Send + Sync> {
        Arc::new(StorageMergeOperator)
    }

    fn entry_key(&self, key: Digest) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.prefix.len() + 7 + 32);
        buf.extend_from_slice(self.prefix.as_bytes());
        buf.extend_from_slice(b"sha256/");
        buf.extend_from_slice(key.as_ref());
        buf
    }

    fn pending_bytes_key(&self) -> Vec<u8> {
        format!("{}pending_bytes", self.prefix).into_bytes()
    }

    fn pending_keys_key(&self) -> Vec<u8> {
        format!("{}pending_keys", self.prefix).into_bytes()
    }

    /// Test-only: `staged` finds pending entries via `pending_keys_key`,
    /// not by scanning this range.
    #[cfg(test)]
    fn entry_prefix(&self) -> Vec<u8> {
        format!("{}sha256/", self.prefix).into_bytes()
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

    /// Whether `key` is already stored, without fetching its value.
    async fn contains(&self, key: Digest) -> io::Result<bool> {
        Ok(self.db.get(self.entry_key(key)).await.map_err(other)?.is_some())
    }

    /// Fetch and decode the entry stored under `key`, if present.
    async fn get(&self, key: Digest) -> io::Result<Option<Entry>> {
        let Some(raw) = self.db.get(self.entry_key(key)).await.map_err(other)? else {
            return Ok(None);
        };
        Entry::decode(&raw).map(Some)
    }

    /// Stage `bytes` under `key`, durable immediately -- not yet in a pack.
    async fn put(&self, key: Digest, bytes: Bytes) -> io::Result<()> {
        let len = bytes.len() as u64;
        let entry = Entry::Inline(bytes);
        let mut batch = WriteBatch::new();
        batch.put_bytes(Bytes::from(self.entry_key(key)), entry.encode());
        batch.merge(self.pending_bytes_key(), len.to_be_bytes());
        batch.merge(self.pending_keys_key(), key.as_ref());
        self.db.write(batch).await.map_err(other)?;
        Ok(())
    }

    /// How many distinct keys have ever been stored (staged or packed)
    /// under this stage's prefix. Test-only.
    #[cfg(test)]
    async fn entry_count(&self) -> io::Result<usize> {
        let mut iter = self.db.scan_prefix(self.entry_prefix(), ..).await.map_err(other)?;
        let mut n = 0;
        while iter.next().await.map_err(other)?.is_some() {
            n += 1;
        }
        Ok(n)
    }

    /// Currently-staged entries (per `pending_keys_key` -- not a scan
    /// over every entry ever held, packed or not), with their still-
    /// `Inline` bytes.
    async fn staged(&self) -> io::Result<Vec<(Digest, Bytes)>> {
        let mut staged = Vec::new();
        for digest in self.pending_keys().await? {
            let Some(entry) = self.get(digest).await? else {
                // This is purely internal bookkeeping: `put` always
                // writes the entry before merging its digest into
                // pending_keys, and entries are never deleted, so
                // reaching here would mean that invariant itself broke.
                // Still safe to skip. Nothing to do for a missing entry.
                continue;
            };
            let Entry::Inline(bytes) = entry else {
                // `flushing` ensures only one `flush_pending` call runs
                // at a time, and only `commit_packed` flips Inline to Packed,
                // so reaching here would mean that serialization itself broke.
                // Still safe to skip. Nothing to do for an already packed entry.
                continue;
            };
            staged.push((digest, bytes));
        }
        Ok(staged)
    }

    /// Atomically flips `entries` to `Packed` and resets the pending
    /// counters -- called once their bytes are durable in a pack object.
    async fn commit_packed(&self, entries: Vec<(Digest, Entry)>) -> io::Result<()> {
        let mut batch = WriteBatch::new();
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
}

impl Packs {
    fn path(&self, pack_id: Digest) -> Path {
        Path::from(self.prefix.as_str()).join("sha256").join(format!("{pack_id:x}"))
    }

    /// Fetch `length` bytes at `offset` from pack `pack_id`.
    async fn get_range(&self, pack_id: Digest, offset: u64, length: u64) -> io::Result<Bytes> {
        let range: Range<u64> = offset..offset + length;
        let opts = GetOptions { range: Some(range.into()), ..Default::default() };
        let result =
            self.store.get_opts(&self.path(pack_id), opts).await.map_err(io::Error::from)?;
        Ok(result.bytes().await?)
    }

    /// Write `payload` as one new pack object identified by `pack_id`.
    async fn write(&self, pack_id: Digest, payload: PutPayload) -> io::Result<()> {
        self.store.put(&self.path(pack_id), payload).await.map_err(io::Error::from)?;
        Ok(())
    }
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
