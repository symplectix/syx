//! cas: content-addressed storage.
use std::io;

pub use bytes::Bytes;
pub use hash::{
    Digest,
    FromBytes,
    Hasher,
    ToBytes,
    digest,
};
pub use storage::{
    Backend,
    Storage,
};
use tokio::io::{
    AsyncRead,
    AsyncWrite,
};

mod hash;
mod storage;

/// Reads the content at `digest`, if present.
pub async fn get<S, T>(backend: &S, digest: &Digest) -> io::Result<Option<T>>
where
    S: Backend,
    T: FromBytes,
{
    Storage::new(backend).get(digest).await
}

/// Store `content`, addressed by its own digest, and return that
/// digest. A thin wrapper over `copy_from`, over the already
/// in-memory bytes.
pub async fn put<S, T>(backend: &S, content: &T) -> io::Result<Digest>
where
    S: Backend,
    T: ToBytes,
{
    Storage::new(backend).put(content).await
}

/// Reads the content at `digest` if present and write it to `w`.
///
/// `get` is the better choice for values small enough that this doesn't matter.
pub async fn read_into<S, W>(backend: &S, digest: &Digest, w: &mut W) -> io::Result<bool>
where
    S: Backend,
    W: AsyncWrite + Unpin,
{
    Storage::new(backend).read_into(digest, w).await
}

/// Store the content read from `r` of `len` bytes, addressed by its own
/// digest.
pub async fn copy_from<S, R>(backend: &S, len: u64, r: &mut R) -> io::Result<Digest>
where
    S: Backend,
    R: AsyncRead + Unpin,
{
    Storage::new(backend).copy_from(len, r).await
}
