//! Content-addressed blob storage: chunking, encoding on the way in,
//! decoding and verifying on the way out. Blobs are staged in `slatedb`
//! before being consolidated into pack objects in a wrapped `ObjectStore`.
//!
//! Fabric-internal: `cas` used to own this (as `cas::Storage`), but `cas`
//! had exactly one consumer -- `fabric`. `cas` keeps the storage-agnostic
//! pieces (`Digest`, `Chunking`, `Codec`); this module keeps the part
//! that's actually about `slatedb`/`object_store`.
//!
//! `Graph` (see `graph.rs`) holds `db`/`stage`/`packs`/`chunking`/`codec`
//! as its own fields -- there's no `Storage` type bundling them up. What
//! it hands out instead is `Cas<'_>`, a borrowed view constructed fresh
//! per call (`Graph::cas()`): all the blob-storage methods live there,
//! so `Graph`'s own methods don't have to be the blob-storage API
//! directly (leaves room for e.g. `Graph::links()` later without name
//! collisions), and every method on `Cas` reaches `db` via `self.db`
//! instead of taking it as a parameter.
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
//! (`Stage`/`Packs`'s own `prefix` field, default `cas/`, see [`Builder::DEFAULT_PREFIX`]).
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
//! The content exactly as [`cas::Codec::encode`] produced it (`[payload][ContentFlags]`), plus one
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
use cas::{
    Chunking,
    Codec,
    ContentFlags,
    Digest,
    FromBytes,
    Hasher,
    ToBytes,
};
use futures::StreamExt as _;
use object_store::{
    ObjectStore,
    PutPayload,
};
use tokio::io::{
    AsyncRead,
    AsyncWrite,
    AsyncWriteExt as _,
};
use tokio::task;

mod packs;
mod stage;

#[cfg(test)]
mod tests;

fn invalid_data(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

fn other(e: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::other(e)
}

/// The merge operator `db` must be opened with for `Cas` to work
/// correctly against it. `Cas`/`Graph` never open `db` themselves --
/// whoever does (`Graph::Builder`) registers this.
pub(crate) fn merge_operator() -> Box<dyn slatedb::MergeOperator + Send + Sync> {
    Stage::merge_operator()
}

/// The default `prefix`, for the common case of `packs` existing solely
/// for this `Graph`'s own blob storage.
pub(crate) const DEFAULT_PREFIX: &str = "cas/";

/// The default `packs_threshold`: 32 MiB -- enough to consolidate
/// several dozen chunks per pack.
pub(crate) const DEFAULT_PACKS_THRESHOLD: u64 = Chunking::AVG_SIZE as u64 * 64;

/// Assembles the blob-storage parts (`stage`/`packs`/`chunking`/`codec`)
/// for `Graph` to hold directly. `Graph::Builder` is the configuration
/// surface (defaults, overrides) -- this just builds what it resolves,
/// so there's no second builder re-exposing the same knobs one layer
/// down for no added behavior.
pub(crate) fn parts(
    packs_backend: Arc<dyn ObjectStore>,
    prefix: String,
    packs_threshold: u64,
) -> Parts {
    Parts {
        stage: Stage {
            prefix:         prefix.clone(),
            flushing:       Arc::new(tokio::sync::Mutex::new(())),
            flush_failures: Arc::new(AtomicU32::new(0)),
        },
        packs: Packs { store: packs_backend, prefix, threshold: packs_threshold },
    }
}

/// The blob-storage facet of a `Graph`: chunking, encoding/decoding, and
/// physical storage of blobs staged in `db` and packed into `packs` over
/// time. A borrowed view, not an owned type -- `Graph` holds `db`/
/// `stage`/`packs`/`chunking`/`codec` itself and builds one of these
/// fresh per call via `Graph::cas()`. `Copy`, since every field either
/// is a reference or is itself `Copy` -- cheap to pass around by value.
#[derive(Clone, Copy)]
pub struct Cas<'a> {
    db:       &'a slatedb::Db,
    stage:    &'a Stage,
    packs:    &'a Packs,
    chunking: Chunking,
    codec:    Codec,
}

/// What `parts` produces, for `Graph::new` to destructure into its own
/// fields. `chunking`/`codec` aren't here: they pass straight through
/// unchanged, so every caller already has its own copy and doesn't need
/// one handed back.
pub(crate) struct Parts {
    pub(crate) stage: Stage,
    pub(crate) packs: Packs,
}

/// The staging area within whichever `db` each `Cas` call is given --
/// entries land here first, before being consolidated into `Packs`.
#[derive(Clone)]
pub(crate) struct Stage {
    /// This stage's own namespace within `db`.
    prefix:         String,
    /// Serializes `flush_pending`.
    flushing:       Arc<tokio::sync::Mutex<()>>,
    /// Consecutive `flush_pending` failures, reset to 0 on success.
    /// `put_blob`'s opportunistic call reads this to decide whether to
    /// swallow an error or propagate it.
    flush_failures: Arc<AtomicU32>,
}

/// Where staged entries get consolidated into once `threshold`
/// accumulates.
#[derive(Clone)]
pub(crate) struct Packs {
    store:     Arc<dyn ObjectStore>,
    prefix:    String,
    threshold: u64,
}

/// Where an entry currently lives.
enum Entry {
    /// Still staged: the raw bytes themselves -- opaque here, but
    /// really `[payload][ContentFlags]`, as `Codec::encode`
    /// produced it.
    Inline(Bytes),
    /// Migrated: where to find it in an already-durable pack.
    Packed { pack_id: Digest, offset: u64, length: u64 },
}

impl<'a> Cas<'a> {
    /// How many consecutive `flush_pending` failures `put_blob` tolerates.
    const MAX_CONSECUTIVE_FLUSH_FAILURES: u32 = 3;

    /// Builds a view over `db`/`stage`/`packs`/`chunking`/`codec` --
    /// only `Graph::cas()` calls this, see the module doc.
    pub(crate) fn new(
        db: &'a slatedb::Db,
        stage: &'a Stage,
        packs: &'a Packs,
        chunking: Chunking,
        codec: Codec,
    ) -> Self {
        Self { db, stage, packs, chunking, codec }
    }

    /// Whether `key` is already stored, without fetching its value.
    async fn contains_blob(&self, key: Digest) -> io::Result<bool> {
        self.stage.contains(self.db, key).await
    }

    /// Fetch bytes stored under `key`, if present.
    async fn get_blob(&self, key: Digest) -> io::Result<Option<Bytes>> {
        let Some(entry) = self.stage.get(self.db, key).await? else {
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
        if self.stage.pending_bytes(self.db).await? >= self.packs.threshold
            && let Err(e) = self.flush_pending().await
        {
            let failures = self.stage.flush_failures.load(Ordering::Relaxed);
            if failures >= Self::MAX_CONSECUTIVE_FLUSH_FAILURES {
                return Err(e);
            }
        }
        self.stage.put(self.db, key, bytes).await
    }

    /// How many distinct keys have ever been staged or packed
    /// under this store's prefix. Test-only.
    #[cfg(test)]
    pub(crate) async fn entry_count(&self) -> io::Result<usize> {
        self.stage.entry_count(self.db).await
    }

    /// Consolidates all currently-staged entries into one new pack object.
    ///
    /// If another call is already in progress, this returns immediately
    /// without doing anything, rather than waiting its turn.
    pub async fn flush_pending(&self) -> io::Result<()> {
        let Ok(_guard) = self.stage.flushing.try_lock() else {
            return Ok(());
        };

        let staged = self.stage.staged(self.db).await?;
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
            Ok(()) => self.stage.commit_packed(self.db, entries).await,
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
