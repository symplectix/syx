//! The backend a blob is actually stored on.
use std::future::Future;
use std::io;

use bytes::Bytes;

/// The backend-specific half: moves already-encoded bytes in and out
/// by a key the caller supplies. Chunking, manifest encoding/decoding,
/// digest computation and verification, and compression all live one
/// layer up, in `blobs`. A `Storage` impl doesn't interpret `key` or
/// `bytes`, it just stores bytes under bytes.
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
