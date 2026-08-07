//! `Repository`: ply's content-addressed store.

use std::io;
use std::path::Path;

use tokio::io::{
    AsyncRead,
    AsyncWrite,
};

/// ply's content-addressed store.
#[derive(Clone)]
pub struct Repository {
    cas: cas::Storage<cas_fjall::Store>,
}

impl Repository {
    /// Open a store at `root`, creating it if it doesn't already exist.
    pub fn open(root: impl AsRef<Path>, cache_bytes: u64) -> io::Result<Self> {
        Ok(Self { cas: cas::Storage::new(cas_fjall::Store::open(root, cache_bytes)?) })
    }

    /// Reads the content at `digest`, if present.
    pub async fn get<T: cas::FromBytes>(&self, digest: &cas::Digest) -> io::Result<Option<T>> {
        self.cas.get(digest).await
    }

    /// Reads the content at `digest` if present and write it to `w`.
    ///
    /// `get` is the better choice for values small enough that this doesn't matter.
    pub async fn read_into<W>(&self, digest: &cas::Digest, w: &mut W) -> io::Result<bool>
    where
        W: AsyncWrite + Unpin,
    {
        self.cas.read_into(digest, w).await
    }

    /// Store `content`, addressed by its own digest, and return that
    /// digest. A thin wrapper over `copy_from`.
    pub async fn put<T: cas::ToBytes>(&self, content: &T) -> io::Result<cas::Digest> {
        self.cas.put(content).await
    }

    /// Store the content read from `r` of `len` bytes, addressed by its
    /// own digest.
    pub async fn copy_from<R>(&self, len: u64, r: &mut R) -> io::Result<cas::Digest>
    where
        R: AsyncRead + Unpin,
    {
        self.cas.copy_from(len, r).await
    }
}
