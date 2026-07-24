//! A fjall-backed `cas::Backend`, and `ply`'s content-addressed store.

use std::io;
use std::path::Path;

use cas::Bytes;
use tokio::io::{
    AsyncRead,
    AsyncWrite,
};
use tokio::task;

/// A content-addressed store mapping `Digest` keys to blob content.
///
/// TODO: no GC yet. A blob's manifest is always written after all the
/// chunks it references, so the only state a crash (or a reader racing
/// a concurrent write) can observe is a chunk that was never adopted by
/// any manifest -- safe to eventually sweep, never a manifest pointing
/// at missing data. GC roots are expected to come from outside this
/// crate (e.g. a log of which digests are still in use).
// TODO: support more operations
// - A gRPC service exposing `Store` via REAPI protos like:
//   - `bytestream.ByteStream.Read` for ranged blob reads
//   - `ContentAddressableStorage.GetTree` for tree structure
// - Explicit CLI-driven blob handling.
// - groupcache-style cache sync between `Store` peers.
// - Durability sync to a remote backend (e.g. S3).
#[derive(Clone)]
pub struct Store {
    // Not read after construction for now.
    _db: fjall::Database,
    cas: fjall::Keyspace,
}

impl cas::Backend for Store {
    // fjall's own API is blocking, so each method hops onto the
    // blocking pool itself -- `cas`'s generic algorithms no longer
    // impose that on every backend.

    async fn contains_blob(&self, key: &[u8]) -> io::Result<bool> {
        let cas = self.cas.clone();
        let key = key.to_vec();
        task::spawn_blocking(move || cas.contains_key(key).map_err(fjall_to_io))
            .await
            .expect("blocking task should not panic")
    }

    async fn get_blob(&self, key: &[u8]) -> io::Result<Option<Bytes>> {
        let cas = self.cas.clone();
        let key = key.to_vec();
        task::spawn_blocking(move || Ok(cas.get(key).map_err(fjall_to_io)?.map(Bytes::from)))
            .await
            .expect("blocking task should not panic")
    }

    async fn put_blob(&self, key: &[u8], bytes: Bytes) -> io::Result<()> {
        let cas = self.cas.clone();
        let key = key.to_vec();
        task::spawn_blocking(move || cas.insert(key, bytes).map_err(fjall_to_io))
            .await
            .expect("blocking task should not panic")
    }
}

impl Store {
    const CAS_KEYSPACE: &str = "cas";

    /// Open a store at `root`, creating it if it doesn't already exist.
    pub fn open(root: impl AsRef<Path>, cache_bytes: u64) -> io::Result<Self> {
        let db = fjall::Database::builder(root)
            // The block cache capacity should be ~20-25% of the available memory
            // - or more if the data set fully fits into memory.
            .cache_size(cache_bytes)
            // `cas` already zstd-compresses its chunks itself, and
            // they're always well over the journal's 4 KiB compression
            // threshold, so compressing them again in the journal
            // would just spend CPU for no benefit.
            .journal_compression(fjall::CompressionType::None)
            .open()
            .map_err(fjall_to_io)?;
        let cas = db
            .keyspace(Self::CAS_KEYSPACE, || {
                fjall::KeyspaceCreateOptions::default()
                    // Without KV separation, the LSM tree copies their full
                    // bytes on every compaction pass. Blob separation moves
                    // them into their own append-only files that compaction
                    // never has to rewrite, only occasionally GC.
                    .with_kv_separation(Some(
                        fjall::KvSeparationOptions::default()
                            // `cas` already zstd-compresses chunks itself.
                            .compression(fjall::CompressionType::None)
                            // Keeps manifests for typical blobs inline in the LSM tree.
                            .separation_threshold(2 * 1024),
                    ))
                    .data_block_compression_policy(fjall::config::CompressionPolicy::disabled())
            })
            .map_err(fjall_to_io)?;
        Ok(Store { _db: db, cas })
    }

    /// Reads the content at `digest`, if present.
    pub async fn get<T: cas::FromBytes>(&self, digest: &cas::Digest) -> io::Result<Option<T>> {
        cas::get(self, digest).await
    }

    /// Reads the content at `digest` if present and write it to `w`.
    ///
    /// `get` is the better choice for values small enough that this doesn't matter.
    pub async fn read_into<W>(&self, digest: &cas::Digest, w: &mut W) -> io::Result<bool>
    where
        W: AsyncWrite + Unpin,
    {
        cas::read_into(self, digest, w).await
    }

    /// Store `content`, addressed by its own digest, and return that
    /// digest. A thin wrapper over `copy_from`.
    pub async fn put<T: cas::ToBytes>(&self, content: &T) -> io::Result<cas::Digest> {
        cas::put(self, content).await
    }

    /// Store the content read from `r` of `len` bytes, addressed by its
    /// own digest. A thin wrapper over the free `cas::copy_from`.
    pub async fn copy_from<R>(&self, len: u64, r: &mut R) -> io::Result<cas::Digest>
    where
        R: AsyncRead + Unpin,
    {
        cas::copy_from(self, len, r).await
    }
}

fn fjall_to_io(e: fjall::Error) -> io::Error {
    io::Error::other(e)
}
