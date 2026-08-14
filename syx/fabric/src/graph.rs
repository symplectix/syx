//! A content-addressable (hyper)graph.
use std::io;

use tokio::io::{
    AsyncRead,
    AsyncWrite,
};

/// A content-addressable (hyper)graph.
///
/// `Graph` is not a database, it's git for your application's data:
/// - not just files but any fact
/// - not just commits a human makes but any derivations a Function makes
#[derive(Clone)]
pub struct Graph {
    /// `Graph` is built directly on `cas::Storage`, so a relation's own source
    /// material lives in the same content-addressed space as the relation
    /// itself, not in a separate system.
    /// - One ingestion pipeline, two consequences for free: store the source as a blob, run
    ///   extraction (a Function), write the resulting relations against that digest. Ingestion
    ///   itself is just a relation between the graph and an external resource, the same mechanism
    ///   any other derivation uses. That gets: no external store to sync with, since a relation's
    ///   source lives inside the graph itself; and lineage all the way back to the true source for
    ///   free, no separate provenance mechanism needed.
    /// - Re-extraction never re-fetches anything: the source is pinned by digest forever, so
    ///   changing extraction logic and rerunning it just adds new relations against the same
    ///   source, old ones left intact.
    cas: cas::Storage,
}

impl Graph {
    /// Wraps `cas`.
    pub const fn new(cas: cas::Storage) -> Self {
        Self { cas }
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
