//! Operations over blobs, bound to one `Backend` -- chunking and
//! encoding on the way in, decoding and verifying on the way out.
//! `backend` is bound once here instead of being threaded through
//! every call, since `get`/`put`/`read_into`/`copy_from` all need it.
//! The free functions in `super` construct one of these per call; this
//! type itself stays private to `blob`.
use std::io;
use std::pin::pin;

use bitflags::bitflags;
use bytes::{
    Buf,
    BufMut,
    Bytes,
};
use fastcdc::v2020;
use futures::StreamExt as _;
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
    /// The trailing byte of every entry's stored payload.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) struct Flags: u8 {
        /// The payload that follows is compressed by zstd.
        const COMPRESSED = 1 << 0;
        /// The payload is a manifest (an ordered list of ChunkRef),
        /// not content itself.
        const MANIFEST = 1 << 1;
    }
}

/// Chunking and encoding on the way in,
/// decoding and verifying on the way out.
pub struct Storage<T> {
    backend:  T,
    chunking: Chunking,
    encoding: Encoding,
    decoding: Decoding,
}

/// Moves bytes in and out by a key the caller supplies.
/// A `Backend` doesn't interpret `key` or `bytes`,
/// it just stores bytes under bytes.
pub trait Backend: Sync {
    /// Whether `key` is already stored, without fetching its value.
    fn contains_blob(&self, key: &[u8]) -> impl Future<Output = io::Result<bool>> + Send;

    /// Fetch bytes stored under `key`, if present.
    fn get_blob(&self, key: &[u8]) -> impl Future<Output = io::Result<Option<Bytes>>> + Send;

    /// Store `bytes` under `key`.
    fn put_blob(&self, key: &[u8], bytes: Bytes) -> impl Future<Output = io::Result<()>> + Send;
}

impl<T: Backend> Backend for &T {
    fn contains_blob(&self, key: &[u8]) -> impl Future<Output = io::Result<bool>> + Send {
        (*self).contains_blob(key)
    }

    fn get_blob(&self, key: &[u8]) -> impl Future<Output = io::Result<Option<Bytes>>> + Send {
        (*self).get_blob(key)
    }

    fn put_blob(&self, key: &[u8], bytes: Bytes) -> impl Future<Output = io::Result<()>> + Send {
        (*self).put_blob(key, bytes)
    }
}

impl<T: Backend> Storage<T> {
    /// Wraps `backend`.
    pub const fn new(backend: T) -> Self {
        Self {
            backend,
            chunking: Chunking::new(),
            encoding: Encoding::new(),
            decoding: Decoding::new(),
        }
    }

    /// Write one entry under `key`, skipping the encode step entirely
    /// if `key` is already stored.
    async fn save(&self, key: Digest, bytes: Vec<u8>, flags: Flags) -> io::Result<()> {
        if self.backend.contains_blob(key.as_ref()).await? {
            return Ok(());
        }
        let encoder = self.encoding;

        // Encoding runs in its own `spawn_blocking`, independent of however
        // the backend chooses to run `put_blob` itself: it's CPU-bound work
        // that always needs to stay off the async executor, regardless of
        // which backend `S` is.
        let encoded = task::spawn_blocking(move || encoder.encode(flags, bytes))
            .await
            .expect("encode should not panic");
        self.backend.put_blob(key.as_ref(), Bytes::from(encoded)).await
    }

    /// Fetch and decode one entry (a chunk or a manifest).
    ///
    /// Callers should check the digest themselves, since what it
    /// should be verified against differs for a manifest's own entry vs.
    /// one of the chunks it lists.
    async fn load(&self, digest: &Digest) -> io::Result<Option<(Flags, Bytes)>> {
        let Some(stored) = self.backend.get_blob(digest.as_ref()).await? else {
            return Ok(None);
        };
        let decoder = self.decoding;
        task::spawn_blocking(move || decoder.decode(stored).map(Some))
            .await
            .expect("decode should not panic")
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

        if !flags.contains(Flags::MANIFEST) {
            if digest_of(&decoded) != *digest {
                return Err(invalid_data("direct content digest mismatch"));
            }
            w.write_all(&decoded).await?;
            return Ok(true);
        }

        let manifest = decode_manifest(&decoded)?;
        let recomputed = {
            let mut h = Hasher::new();
            h.parts(manifest.iter().map(|e| e.digest.as_ref()));
            h.digest()
        };
        if recomputed != *digest {
            return Err(invalid_data("manifest digest mismatch"));
        }

        for entry in manifest {
            let Some((chunk_flags, raw)) = self.load(&entry.digest).await? else {
                return Err(invalid_data(format!(
                    "manifest references missing chunk {:x}",
                    entry.digest
                )));
            };
            if chunk_flags.contains(Flags::MANIFEST) {
                return Err(invalid_data(format!(
                    "chunk {:x} entry is itself a manifest",
                    entry.digest
                )));
            }
            if raw.len() as u32 != entry.len {
                return Err(invalid_data(format!("chunk {:x} length mismatch", entry.digest)));
            }
            if digest_of(&raw) != entry.digest {
                return Err(invalid_data(format!(
                    "chunk {:x} content digest mismatch",
                    entry.digest
                )));
            }
            w.write_all(&raw).await?;
        }
        Ok(true)
    }

    /// Store the content read from `r` of `len` bytes, addressed by its own
    /// digest.
    ///
    /// Each chunk is written as soon as it's produced, not buffered in
    /// memory: only the manifest -- an ordered list of chunk digests and
    /// lengths, far smaller than the content itself -- accumulates here, so
    /// peak memory stays bounded regardless of `len`.
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

        // Only the most recent digest is needed to detect the zero/one/many
        // chunks cases below; the multi-chunk blob digest itself is folded
        // in incrementally, so there's no need to collect every chunk
        // digest into a `Vec` just to hash over it afterward. `manifest`
        // already grows by exactly 36 bytes per chunk, so its length alone
        // (rather than a separate counter) tells us how many chunks there
        // were.
        let mut chunk_digests = Hasher::new();
        let mut last_digest = None;
        let mut manifest = Vec::new();
        let mut total: u64 = 0;
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk?;
            total += chunk.length as u64;
            let digest = digest_of(&chunk.data);
            manifest.put_slice(digest.as_ref());
            manifest.put_u32(chunk.length as u32);
            chunk_digests.part(digest.as_ref());
            last_digest = Some(digest);
            self.save(digest, chunk.data, Flags::empty()).await?;
        }

        if total != len {
            // The reader ended before supplying all of `len` bytes.
            //
            // `Take` silently short-reads on early EOF instead of erroring,
            // so this has to be checked explicitly. The chunks already written
            // above are (harmless) orphans -- no manifest ever points at them.
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!("reader ended {} bytes short of the declared length {len}", len - total),
            ));
        }

        if manifest.is_empty() {
            // No chunks were emitted. The length check above already
            // guarantees `total == len`, so this can only mean `len` was 0.
            //
            // A blob digest always needs at least one chunk digest to hash
            // over, so treat empty content as exactly one (empty) chunk instead.
            // Falls through to the single-chunk shortcut below.
            let digest = digest_of(&[]);
            manifest.put_slice(digest.as_ref());
            manifest.put_u32(0);
            last_digest = Some(digest);
            self.save(digest, Vec::new(), Flags::empty()).await?;
        }

        // Each chunk (real or the synthetic empty one above) appends
        // exactly one 36-byte record, so this always holds. This is what
        // makes `manifest.len() == 36` below a reliable way to detect
        // "exactly one chunk" without a separate counter.
        debug_assert!(manifest.len().is_multiple_of(36));

        if manifest.len() == 36 {
            // Exactly one chunk (one 36-byte manifest record) was emitted,
            // so its own digest is already the blob digest -- already
            // written above under that key, so there's nothing left to do.
            // This also means a small blob and the same content appearing
            // as one chunk inside a larger blob dedup against each other.
            Ok(last_digest.expect("manifest.len() == 36 implies last_digest was set"))
        } else {
            let blob_digest = chunk_digests.digest();
            self.save(blob_digest, manifest, Flags::MANIFEST).await?;
            Ok(blob_digest)
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
/// before, with different digests. Existing chunks and manifests stay
/// perfectly readable, but new writes no longer dedup against what's
/// already stored under the old parameters.
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
    pub(super) fn encode(&self, mut flags: Flags, mut bytes: Vec<u8>) -> Vec<u8> {
        let sample = &bytes[..bytes.len().min(self.sniff_len)];
        if self.worth_compressing(sample) {
            let mut compressed = zstd::bulk::compress(&bytes, self.compression_level)
                .expect("zstd compression of an in-memory buffer should not fail");
            flags |= Flags::COMPRESSED;
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
    pub(super) fn decode(&self, stored: Bytes) -> io::Result<(Flags, Bytes)> {
        if stored.is_empty() {
            return Err(invalid_data("stored content is missing its trailing flag byte"));
        }
        let mut bytes = stored.slice(..stored.len() - 1);
        let mut flags = Flags::from_bits_retain(stored[stored.len() - 1]);
        if flags.contains(Flags::COMPRESSED) {
            // `bytes` is decompressed from here on -- the returned flags
            // should describe it.
            flags.remove(Flags::COMPRESSED);
            bytes = Bytes::from(
                zstd::decode_all(bytes.as_ref())
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
            );
        }
        Ok((flags, bytes))
    }
}

/// A reference to one chunk from within a manifest: its digest and its
/// length, so a length mismatch (a cheap check) can be caught before
/// the more expensive digest comparison.
struct ChunkRef {
    digest: Digest,
    len:    u32,
}

/// Decode a manifest body into its ordered chunk references.
///
/// The format is a flat sequence of 36-byte records (`digest[32] || len: u32 be`).
fn decode_manifest(bytes: &[u8]) -> io::Result<Vec<ChunkRef>> {
    if !bytes.len().is_multiple_of(36) {
        return Err(invalid_data("manifest body length is not a multiple of 36"));
    }
    let mut manifest = Vec::with_capacity(bytes.len() / 36);
    let mut buf = bytes;
    let mut digest = [0u8; 32];
    while buf.has_remaining() {
        buf.copy_to_slice(&mut digest);
        manifest.push(ChunkRef { digest: Digest::new(digest), len: buf.get_u32() });
    }
    Ok(manifest)
}

/// This chunk's digest: the same length-prefixed single-part framing
/// `Hasher` uses everywhere else.
pub(super) fn digest_of(chunk: &[u8]) -> Digest {
    let mut h = Hasher::new();
    h.part(chunk);
    h.digest()
}

fn invalid_data(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}
