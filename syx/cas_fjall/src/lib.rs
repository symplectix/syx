//! A fjall-backed `cas::blob::{Exists, Get, Put}`.

use std::io;
use std::path::Path;

use cas::Bytes;
use tokio::task;

/// A fjall-backed blob store.
///
/// TODO: no GC yet. A blob's manifest is always written after all the
/// chunks it references, so the only state a crash (or a reader racing
/// a concurrent write) can observe is a chunk that was never adopted by
/// any manifest -- safe to eventually sweep, never a manifest pointing
/// at missing data. GC roots are expected to come from outside this
/// crate (e.g. a log of which digests are still in use).
#[derive(Clone)]
pub struct Store {
    // Not read after construction for now.
    _db: fjall::Database,
    cas: fjall::Keyspace,
}

impl cas::blob::Exists for Store {
    // fjall's own API is blocking, so each method hops onto the
    // blocking pool itself -- `cas`'s generic algorithms no longer
    // impose that on every backend.

    async fn contains_blob(&self, key: cas::Digest) -> io::Result<bool> {
        let cas = self.cas.clone();
        task::spawn_blocking(move || cas.contains_key(key).map_err(fjall_to_io))
            .await
            .expect("blocking task should not panic")
    }
}

impl cas::blob::Get for Store {
    async fn get_blob(&self, key: cas::Digest) -> io::Result<Option<Bytes>> {
        let cas = self.cas.clone();
        task::spawn_blocking(move || Ok(cas.get(key).map_err(fjall_to_io)?.map(Bytes::from)))
            .await
            .expect("blocking task should not panic")
    }
}

impl cas::blob::Put for Store {
    async fn put_blob(&self, key: cas::Digest, bytes: Bytes) -> io::Result<()> {
        let cas = self.cas.clone();
        task::spawn_blocking(move || cas.insert(key.as_ref(), bytes).map_err(fjall_to_io))
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
}

fn fjall_to_io(e: fjall::Error) -> io::Error {
    io::Error::other(e)
}
