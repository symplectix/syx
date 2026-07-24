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

use super::{
    consts,
    decode_manifest,
    digest_of,
    invalid_data,
};
use crate::hash::{
    Digest,
    FromBytes,
    Hasher,
    ToBytes,
};

bitflags! {
    /// The trailing byte of every entry's stored payload.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) struct Flags: u8 {
        /// The payload that follows is compressed by zstd.
        const COMPRESSED = 1 << 0;
        /// The payload is a manifest (an ordered list of ChunkRef),
        /// not content itself.
        const MANIFEST = 1 << 1;
    }
}

/// The compression knobs from `consts`, as a value instead of
/// globals. `Default` gives back exactly `consts`' values; tests
/// build their own.
#[derive(Clone, Copy)]
pub(super) struct Encoder {
    compression_level: i32,
    sniff_len:         usize,
    sniff_max_ratio:   f64,
}

impl Default for Encoder {
    fn default() -> Self {
        Self::new()
    }
}

impl Encoder {
    pub(super) const fn new() -> Self {
        Self {
            compression_level: consts::COMPRESSION_LEVEL,
            sniff_len:         consts::SNIFF_LEN,
            sniff_max_ratio:   consts::SNIFF_MAX_RATIO,
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

/// The read-side counterpart to `Encoder`.
#[derive(Clone, Copy)]
pub(super) struct Decoder {
    // Unlike encoding, decoding needs no options for now.
}

impl Default for Decoder {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder {
    pub(super) const fn new() -> Self {
        Self {}
    }

    /// The inverse of `Encoder::encode`.
    ///
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

/// Chunking and encoding on the way in,
/// decoding and verifying on the way out.
pub struct Storage<T> {
    backend: T,
    encoder: Encoder,
    decoder: Decoder,

    // The chunk-size knobs. These aren't safe to change carelessly
    // so there's no builder to override them per call.
    chunk_min_size: usize,
    chunk_avg_size: usize,
    chunk_max_size: usize,
}

/// Moves already-encoded bytes in and out by a key the caller supplies.
/// Chunking, manifest encoding/decoding, digest computation and verification,
/// and compression all live one layer up, in `Storage`. A `Backend` impl doesn't
/// interpret `key` or `bytes`, it just stores bytes under bytes.
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
    /// Wraps `backend`, with `Encoder`/`Decoder` and the chunk-size
    /// knobs set to their `consts` defaults.
    pub const fn new(backend: T) -> Self {
        Self {
            backend,
            encoder: Encoder::new(),
            decoder: Decoder::new(),
            chunk_min_size: consts::CHUNK_MIN_SIZE,
            chunk_avg_size: consts::CHUNK_AVG_SIZE,
            chunk_max_size: consts::CHUNK_MAX_SIZE,
        }
    }

    /// Write one entry under `key`, skipping the encode step entirely
    /// if `key` is already stored.
    async fn save(&self, key: Digest, bytes: Vec<u8>, flags: Flags) -> io::Result<()> {
        if self.backend.contains_blob(key.as_ref()).await? {
            return Ok(());
        }
        let encoder = self.encoder;

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
        let decoder = self.decoder;
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
            self.chunk_min_size,
            self.chunk_avg_size,
            self.chunk_max_size,
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
