//! A content-addressed blob store.
use std::future::Future;
use std::io;
use std::pin::pin;

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

use crate::hash::{
    Digest,
    FromBytes,
    Hasher,
    ToBytes,
};

#[cfg(test)]
#[path = "store_test.rs"]
mod tests;

mod consts;
mod entry;

/// The backend-specific half: moves already-encoded bytes in and out
/// by a key the caller supplies. Chunking, manifest encoding/decoding,
/// digest computation and verification, and compression all live one
/// layer up, in the free functions below. A `Storage` impl doesn't
/// interpret `key` or `bytes`, it just stores bytes under bytes.
///
/// Each method returns its own future instead of being `async fn`, so
/// a blocking backend can hop onto `spawn_blocking` itself, while
/// a natively async one just awaits its client directly.
pub trait Storage: Sync {
    /// Whether `key` is already stored, without fetching its value --
    /// lets a caller skip re-encoding (e.g. compressing) content that's
    /// already present.
    fn contains_blob(&self, key: &[u8]) -> impl Future<Output = io::Result<bool>> + Send;

    /// Fetch bytes stored under `key`, if present.
    fn get_blob(&self, key: &[u8]) -> impl Future<Output = io::Result<Option<Bytes>>> + Send;

    /// Store `bytes` under `key`.
    fn put_blob(&self, key: &[u8], bytes: Bytes) -> impl Future<Output = io::Result<()>> + Send;
}

/// Reads the content at `digest`, if present.
pub async fn get<S, T>(storage: &S, digest: &Digest) -> io::Result<Option<T>>
where
    S: Storage,
    T: FromBytes,
{
    let mut bytes = Vec::new();
    if !read_into(storage, digest, &mut bytes).await? {
        return Ok(None);
    }

    let content = T::from_bytes(Bytes::from(bytes))
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, format!("{e:?}")))?;

    Ok(Some(content))
}

/// Store `content`, addressed by its own digest, and return that
/// digest. A thin wrapper over `copy_from`, over the already
/// in-memory bytes.
pub async fn put<S, T>(storage: &S, content: &T) -> io::Result<Digest>
where
    S: Storage,
    T: ToBytes,
{
    let bytes =
        content.to_bytes().unwrap_or_else(|_| panic!("serializing to bytes should not fail"));
    let len = bytes.len() as u64;
    copy_from(storage, len, &mut io::Cursor::new(bytes)).await
}

/// Reads the content at `digest` if present and write it to `w`.
///
/// `get` is the better choice for values small enough that this doesn't matter.
pub async fn read_into<S, W>(storage: &S, digest: &Digest, w: &mut W) -> io::Result<bool>
where
    S: Storage,
    W: AsyncWrite + Unpin,
{
    let decoder = entry::Decoder;
    let Some((flags, decoded)) = decoder.load(storage, *digest).await? else {
        return Ok(false);
    };

    if !flags.contains(entry::Flags::MANIFEST) {
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
        let Some((chunk_flags, raw)) = decoder.load(storage, entry.digest).await? else {
            return Err(invalid_data(format!(
                "manifest references missing chunk {:x}",
                entry.digest
            )));
        };
        if chunk_flags.contains(entry::Flags::MANIFEST) {
            return Err(invalid_data(format!(
                "chunk {:x} entry is itself a manifest",
                entry.digest
            )));
        }
        if raw.len() as u32 != entry.len {
            return Err(invalid_data(format!("chunk {:x} length mismatch", entry.digest)));
        }
        if digest_of(&raw) != entry.digest {
            return Err(invalid_data(format!("chunk {:x} content digest mismatch", entry.digest)));
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
pub async fn copy_from<S, R>(storage: &S, len: u64, r: &mut R) -> io::Result<Digest>
where
    S: Storage,
    R: AsyncRead + Unpin,
{
    // `r` may be a multiplexed/persistent stream where EOF doesn't mark
    // this blob's end, so bound the chunker to exactly `len` bytes
    // rather than reading until EOF.
    let source = r.take(len);
    let mut cdc = v2020::AsyncStreamCDC::new(
        source,
        consts::CHUNK_MIN_SIZE,
        consts::CHUNK_AVG_SIZE,
        consts::CHUNK_MAX_SIZE,
    );
    let mut chunks = pin!(cdc.as_stream());
    let encoder = entry::Encoder::default();

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
        encoder.save(storage, digest, chunk.data, entry::Flags::empty()).await?;
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
        encoder.save(storage, digest, Vec::new(), entry::Flags::empty()).await?;
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
        encoder.save(storage, blob_digest, manifest, entry::Flags::MANIFEST).await?;
        Ok(blob_digest)
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
fn digest_of(chunk: &[u8]) -> Digest {
    let mut h = Hasher::new();
    h.part(chunk);
    h.digest()
}

fn invalid_data(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}
