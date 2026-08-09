//! Content-addressed blob storage: chunking, encoding and digest framing
//! on the way in, decoding and verifying on the way out. Blobs are
//! staged in `slatedb` before being consolidated into pack objects in a
//! wrapped `object_store::ObjectStore`. See tmp/packfile_c.md for the
//! packing design rationale.
use std::io;
use std::ops::Range;
use std::pin::pin;
use std::sync::Arc;

use bitflags::bitflags;
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

#[cfg(test)]
#[path = "storage_test.rs"]
mod tests;

bitflags! {
    /// The trailing byte of a blob's own encoded content -- set once,
    /// at write time, and unchanged from then on regardless of where
    /// that content ends up physically living.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) struct ContentFlags: u8 {
        /// The payload that follows is compressed by zstd.
        const COMPRESSED = 1 << 0;
        /// The payload is chunked, contains an ordered list of Chunk,
        /// not content itself.
        const CHUNKED = 1 << 1;
    }
}

bitflags! {
    /// The trailing byte of `slatedb` value. Never seen by anything
    /// above `Entry`: it says whether that value *is* the content
    /// or a pointer to where the content currently lives instead.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct EntryFlags: u8 {
        /// This entry isn't content -- it's a pointer to where the
        /// content lives instead.
        const PACKED = 1 << 0;
    }
}

/// Where an entry currently lives.
enum Entry {
    /// Still staged: the raw bytes themselves -- opaque here, but
    /// really `[payload][ContentFlags]`, as `Encoding::encode`
    /// produced it.
    Inline(Bytes),
    /// Migrated: where to find it in an already-durable pack.
    Packed { pack_id: Digest, offset: u64, length: u64 },
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
                buf.extend_from_slice(&[EntryFlags::empty().bits()]);
                buf.freeze()
            }
            Entry::Packed { pack_id, offset, length } => {
                let mut buf = BytesMut::with_capacity(32 + 8 + 8 + 1);
                buf.extend_from_slice(pack_id.as_ref());
                buf.extend_from_slice(&offset.to_be_bytes());
                buf.extend_from_slice(&length.to_be_bytes());
                buf.extend_from_slice(&[EntryFlags::PACKED.bits()]);
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

/// `db`'s merge operator. Dispatches on which of `Storage`'s own
/// mergeable values `key` names:
///
/// - `pending_bytes`: sums `u64` operands. Associative, as `MergeOperator` requires: addition
///   trivially is.
/// - anything else (`pending_keys`): appends operands. Also associative -- each operand is a
///   fixed-width 32-byte digest, so concatenation never needs to look at existing content to stay
///   unambiguous.
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

/// Chunking, encoding, and the physical storage of blobs, staged in
/// `slatedb` and packed into `inner` over time.
#[derive(Clone)]
pub struct Storage {
    db: slatedb::Db,
    inner: Arc<dyn ObjectStore>,
    prefix: String,
    target_pack_bytes: u64,

    chunking: Chunking,
    encoding: Encoding,
    decoding: Decoding,
}

/// `StorageBuilder`'s default `prefix`, for the common case of `db`
/// and `inner` existing solely for this `Storage`'s own sake.
const DEFAULT_PREFIX: &str = "p/";

/// Builds a `Storage`. `db` and `inner` are already open, with a
/// `SumU64` merge operator registered on `db` -- see
/// [`Storage::merge_operator`] -- and blobs accumulate under `prefix`
/// (defaults to [`DEFAULT_PREFIX`]; override only when sharing `db`
/// and/or `inner` with something else that needs its own namespace)
/// until `target_pack_bytes` accumulates, at which point they're
/// consolidated into one pack object written to `inner`.
pub struct StorageBuilder {
    db: slatedb::Db,
    inner: Arc<dyn ObjectStore>,
    prefix: String,
    target_pack_bytes: u64,
}

impl StorageBuilder {
    /// The key prefix blobs are staged and packed under. Only needed
    /// when `db` (and/or `inner`) is shared with something else that
    /// needs its own namespace.
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    pub fn build(self) -> Storage {
        Storage {
            db: self.db,
            inner: self.inner,
            prefix: self.prefix,
            target_pack_bytes: self.target_pack_bytes,
            chunking: Chunking::new(),
            encoding: Encoding::new(),
            decoding: Decoding::new(),
        }
    }
}

impl Storage {
    /// Starts building a `Storage` over `db` and `inner`. See
    /// [`StorageBuilder`].
    pub fn builder(
        db: slatedb::Db,
        inner: Arc<dyn ObjectStore>,
        target_pack_bytes: u64,
    ) -> StorageBuilder {
        StorageBuilder { db, inner, prefix: DEFAULT_PREFIX.to_string(), target_pack_bytes }
    }

    /// The merge operator `db` must be opened with for `Storage`'s
    /// pending-bytes counter (and pending-keys list) to work.
    pub fn merge_operator() -> Arc<dyn MergeOperator + Send + Sync> {
        Arc::new(StorageMergeOperator)
    }

    fn entry_key(&self, key: Digest) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.prefix.len() + 2 + 32);
        buf.extend_from_slice(self.prefix.as_bytes());
        buf.extend_from_slice(b"e/");
        buf.extend_from_slice(key.as_ref());
        buf
    }

    fn pending_bytes_key(&self) -> Vec<u8> {
        format!("{}pending_bytes", self.prefix).into_bytes()
    }

    fn pending_keys_key(&self) -> Vec<u8> {
        format!("{}pending_keys", self.prefix).into_bytes()
    }

    /// Test-only: `flush_pending` finds pending entries via
    /// `pending_keys_key`, not by scanning this range.
    #[cfg(test)]
    fn entry_prefix(&self) -> Vec<u8> {
        format!("{}e/", self.prefix).into_bytes()
    }

    fn pack_path(&self, pack_id: Digest) -> Path {
        Path::from(self.prefix.as_str()).join("packs").join(format!("{pack_id:x}"))
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
    async fn contains_blob(&self, key: Digest) -> io::Result<bool> {
        Ok(self.db.get(self.entry_key(key)).await.map_err(other)?.is_some())
    }

    /// Fetch bytes stored under `key`, if present.
    async fn get_blob(&self, key: Digest) -> io::Result<Option<Bytes>> {
        let Some(raw) = self.db.get(self.entry_key(key)).await.map_err(other)? else {
            return Ok(None);
        };
        match Entry::decode(&raw)? {
            Entry::Inline(bytes) => Ok(Some(bytes)),
            Entry::Packed { pack_id, offset, length } => {
                let range: Range<u64> = offset..offset + length;
                let opts = GetOptions { range: Some(range.into()), ..Default::default() };
                let result = self
                    .inner
                    .get_opts(&self.pack_path(pack_id), opts)
                    .await
                    .map_err(io::Error::from)?;
                Ok(Some(result.bytes().await?))
            }
        }
    }

    /// Store `bytes` under `key`, flushing to a pack if that crosses
    /// `target_pack_bytes`.
    async fn put_blob(&self, key: Digest, bytes: Bytes) -> io::Result<()> {
        let len = bytes.len() as u64;
        let entry = Entry::Inline(bytes);
        self.db.put(self.entry_key(key), entry.encode()).await.map_err(other)?;
        self.db.merge(self.pending_bytes_key(), len.to_be_bytes()).await.map_err(other)?;
        self.db
            .merge(self.pending_keys_key(), Bytes::copy_from_slice(key.as_ref()))
            .await
            .map_err(other)?;

        if self.pending_bytes().await? >= self.target_pack_bytes {
            self.flush_pending().await?;
        }
        Ok(())
    }

    /// How many distinct keys have ever been stored (staged or packed)
    /// under this store's prefix. Test-only.
    #[cfg(test)]
    pub(crate) async fn entry_count(&self) -> io::Result<usize> {
        let mut iter = self.db.scan_prefix(self.entry_prefix(), ..).await.map_err(other)?;
        let mut n = 0;
        while iter.next().await.map_err(other)?.is_some() {
            n += 1;
        }
        Ok(n)
    }

    /// Consolidates all currently-staged entries (per `pending_keys_key`
    /// -- not a scan over every entry this store has ever held, packed
    /// or not) into one new pack object, written to `inner`, then flips
    /// each entry's `slatedb` value to `Packed` via one atomic
    /// `WriteBatch`. The pack object write completes before the
    /// `WriteBatch` is attempted (bytes durable before metadata).
    pub async fn flush_pending(&self) -> io::Result<()> {
        // Each `Inline` value is kept as its own `Bytes`, not copied into
        // one contiguous buffer -- `PutPayload` is a cheaply cloneable
        // sequence of `Bytes` (`Arc<[Bytes]>`), so `inner.put` below
        // takes them as-is.
        let mut chunks = Vec::new();
        let mut total: u64 = 0;
        let mut placements: Vec<(Bytes, u64, u64)> = Vec::new();
        for digest in self.pending_keys().await? {
            let Some(raw) = self.db.get(self.entry_key(digest)).await.map_err(other)? else {
                // Merged into pending_keys_key but no longer present --
                // shouldn't normally happen, but nothing to pack either way.
                continue;
            };
            let Entry::Inline(bytes) = Entry::decode(&raw)? else {
                // Already packed; nothing to do for this entry.
                continue;
            };
            let offset = total;
            let length = bytes.len() as u64;
            total += length;
            placements.push((Bytes::from(self.entry_key(digest)), offset, length));
            chunks.push(bytes);
        }

        let pack_id = Hasher::new().parts(chunks.iter().map(|b| b.as_ref())).digest();
        let mut batch = WriteBatch::new();
        for (key, offset, length) in &placements {
            let entry = Entry::Packed { pack_id, offset: *offset, length: *length };
            batch.put_bytes(key.clone(), entry.encode());
        }

        if batch.is_empty() {
            return Ok(());
        }

        self.inner
            .put(&self.pack_path(pack_id), PutPayload::from_iter(chunks))
            .await
            .map_err(io::Error::from)?;

        batch.put_bytes(
            Bytes::from(self.pending_bytes_key()),
            Bytes::copy_from_slice(&0u64.to_be_bytes()),
        );
        batch.put_bytes(Bytes::from(self.pending_keys_key()), Bytes::new());
        self.db.write(batch).await.map_err(other)?;

        Ok(())
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
    async fn save(&self, key: Digest, bytes: Vec<u8>, flags: ContentFlags) -> io::Result<()> {
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
            self.save(digest, chunk.data, ContentFlags::empty()).await?;
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
            self.save(digest, Vec::new(), ContentFlags::empty()).await?;
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
            self.save(chunks_digest, chunks_bytes, ContentFlags::CHUNKED).await?;
            Ok(chunks_digest)
        }
    }
}

pub(crate) mod defaults {
    //! Default values for chunking and encoding.
    //!
    //! # Chunking: `CHUNK_MIN_SIZE`, `CHUNK_AVG_SIZE`, `CHUNK_MAX_SIZE`
    //!
    //! NOT safe to change without consequence -- see `Chunking` for why.
    //! min/avg set the dedup-vs-compression tradeoff: smaller chunks
    //! dedup more precisely (a small change in content only invalidates
    //! a small chunk) but compress worse (less context per chunk for
    //! zstd to find matches in, plus more per-chunk framing overhead);
    //! larger chunks compress better but dedup more coarsely (one
    //! changed byte invalidates the whole chunk it falls in).
    //!
    //! # Encoding: `COMPRESSION_LEVEL`, `SNIFF_LEN`, `SNIFF_MAX_RATIO`
    //!
    //! Safe to change at any time. Each is a pure write-time heuristic:
    //! every stored chunk records its own compressed-or-not decision,
    //! so changing these only affects future writes, never how existing
    //! ones are read back.
    //!
    //! `SNIFF_LEN`/`SNIFF_MAX_RATIO` trade CPU against storage savings:
    //! stricter (a lower `SNIFF_MAX_RATIO`) only bothers compressing
    //! chunks with a clearly worthwhile payoff, saving CPU but leaving
    //! some real (if modest) compression on the table; looser values
    //! capture more of those marginal savings but spend more CPU chasing
    //! chunks that barely shrink.

    /// One step above zstd's own default level (3), trading a bit more
    /// CPU for a bit better ratio. Worth it here because `SNIFF_MAX_RATIO`
    /// already filters out content that isn't worth compressing in the
    /// first place, so every chunk that reaches this point already has
    /// a real payoff to chase.
    pub(crate) const COMPRESSION_LEVEL: i32 = 4;

    /// How many bytes of a chunk to sample before deciding whether
    /// it's worth compressing.
    pub(crate) const SNIFF_LEN: usize = 16 * 1024;

    /// Skip compression if the sniffed sample doesn't shrink to less
    /// than this fraction of its own size. Already-compressed content
    /// typically doesn't shrink further, so this avoids paying to
    /// compress the rest of it for nothing.
    pub(crate) const SNIFF_MAX_RATIO: f64 = 0.95;

    pub(crate) const CHUNK_MIN_SIZE: usize = SNIFF_LEN * 4;
    pub(crate) const CHUNK_AVG_SIZE: usize = CHUNK_MIN_SIZE * 8;
    pub(crate) const CHUNK_MAX_SIZE: usize = CHUNK_AVG_SIZE * 4;

    const _: () = assert!(
        SNIFF_LEN < CHUNK_MIN_SIZE,
        "SNIFF_LEN must stay smaller than `CHUNK_MIN_SIZE`: \
        otherwise every regular chunk would have its whole content \"sampled\" \
        -- compressed once to decide, then compressed again from scratch."
    );

    const _: () = assert!(
        CHUNK_MAX_SIZE <= 4 * 1024 * 1024,
        "CHUNK_MAX_SIZE has a hard ceiling to respect: \
        gRPC's default max message size is 4MB, and a chunk is expected to \
        map to one message on the wire, so this should stay comfortably under that. \
        Not just below 4MB, leave room for message framing overhead too.",
    );
}

/// The chunk-size knobs.
///
/// These aren't safe to change carelessly. Chunk boundaries depend on
/// these parameters, so changing them shifts where cuts fall:
/// even byte-identical content gets split into different chunks than
/// before, with different digests. Existing chunks stay perfectly readable,
/// but new writes no longer dedup against what's already stored.
#[derive(Clone, Copy)]
pub(super) struct Chunking {
    min_size: usize,
    avg_size: usize,
    max_size: usize,
}

impl Default for Chunking {
    fn default() -> Self {
        Self::new()
    }
}

impl Chunking {
    pub(super) const fn new() -> Self {
        Self {
            min_size: defaults::CHUNK_MIN_SIZE,
            avg_size: defaults::CHUNK_AVG_SIZE,
            max_size: defaults::CHUNK_MAX_SIZE,
        }
    }
}

/// How to encode the chunk.
#[derive(Clone, Copy)]
pub(super) struct Encoding {
    compression_level: i32,
    sniff_len:         usize,
    sniff_max_ratio:   f64,
}

impl Default for Encoding {
    fn default() -> Self {
        Self::new()
    }
}

impl Encoding {
    pub(super) const fn new() -> Self {
        Self {
            compression_level: defaults::COMPRESSION_LEVEL,
            sniff_len:         defaults::SNIFF_LEN,
            sniff_max_ratio:   defaults::SNIFF_MAX_RATIO,
        }
    }

    #[allow(dead_code)]
    pub(super) const fn compression_level(mut self, level: i32) -> Self {
        self.compression_level = level;
        self
    }

    #[allow(dead_code)]
    pub(super) const fn sniff_len(mut self, len: usize) -> Self {
        self.sniff_len = len;
        self
    }

    #[allow(dead_code)]
    pub(super) const fn sniff_max_ratio(mut self, ratio: f64) -> Self {
        self.sniff_max_ratio = ratio;
        self
    }

    /// Compress `bytes` with zstd if that's worthwhile, and append a
    /// flag byte recording whether it was.
    pub(super) fn encode(&self, mut flags: ContentFlags, mut bytes: Vec<u8>) -> Vec<u8> {
        let sample = &bytes[..bytes.len().min(self.sniff_len)];
        if self.worth_compressing(sample) {
            let mut compressed = zstd::bulk::compress(&bytes, self.compression_level)
                .expect("zstd compression of an in-memory buffer should not fail");
            flags |= ContentFlags::COMPRESSED;
            compressed.push(flags.bits());
            return compressed;
        }
        bytes.push(flags.bits());
        bytes
    }

    /// Whether compressing `sample` shrinks it enough to be worth
    /// compressing the rest of the chunk it was taken from.
    pub(super) fn worth_compressing(&self, sample: &[u8]) -> bool {
        if sample.is_empty() {
            return false;
        }
        let compressed_len =
            zstd::bulk::compress(sample, self.compression_level).map_or(sample.len(), |c| c.len());
        (compressed_len as f64) < (sample.len() as f64) * self.sniff_max_ratio
    }
}

/// How to decode the chunk.
/// The read-side counterpart to `Encoding`.
#[derive(Clone, Copy)]
pub(super) struct Decoding {
    // Unlike encoding, decoding needs no options for now.
}

impl Default for Decoding {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoding {
    pub(super) const fn new() -> Self {
        Self {}
    }

    /// The not-worth-compressing case is just a cheap sub-slice
    /// of the already-allocated buffer.
    pub(super) fn decode(&self, stored: Bytes) -> io::Result<(ContentFlags, Bytes)> {
        if stored.is_empty() {
            return Err(invalid_data("stored content is missing its trailing flag byte"));
        }
        let mut bytes = stored.slice(..stored.len() - 1);
        let flags = ContentFlags::from_bits_retain(stored[stored.len() - 1]);
        if flags.contains(ContentFlags::COMPRESSED) {
            bytes = Bytes::from(
                zstd::decode_all(bytes.as_ref())
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
            );
        }
        Ok((flags, bytes))
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

fn invalid_data(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

fn other(e: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::other(e)
}
