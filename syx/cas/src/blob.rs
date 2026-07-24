//! A content-addressed blob store.
use std::io;

use bytes::Buf;
use tokio::io::{
    AsyncRead,
    AsyncWrite,
};

use crate::hash::{
    Digest,
    FromBytes,
    Hasher,
    ToBytes,
};

#[cfg(test)]
mod tests;

mod blobs;
mod consts;
mod entry;
mod storage;

pub use storage::Storage;

/// Reads the content at `digest`, if present.
pub async fn get<S, T>(storage: &S, digest: &Digest) -> io::Result<Option<T>>
where
    S: Storage,
    T: FromBytes,
{
    blobs::Blobs::new(storage).get(digest).await
}

/// Store `content`, addressed by its own digest, and return that
/// digest. A thin wrapper over `copy_from`, over the already
/// in-memory bytes.
pub async fn put<S, T>(storage: &S, content: &T) -> io::Result<Digest>
where
    S: Storage,
    T: ToBytes,
{
    blobs::Blobs::new(storage).put(content).await
}

/// Reads the content at `digest` if present and write it to `w`.
///
/// `get` is the better choice for values small enough that this doesn't matter.
pub async fn read_into<S, W>(storage: &S, digest: &Digest, w: &mut W) -> io::Result<bool>
where
    S: Storage,
    W: AsyncWrite + Unpin,
{
    blobs::Blobs::new(storage).read_into(digest, w).await
}

/// Store the content read from `r` of `len` bytes, addressed by its own
/// digest.
pub async fn copy_from<S, R>(storage: &S, len: u64, r: &mut R) -> io::Result<Digest>
where
    S: Storage,
    R: AsyncRead + Unpin,
{
    blobs::Blobs::new(storage).copy_from(len, r).await
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
